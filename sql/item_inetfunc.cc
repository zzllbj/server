/* Copyright (c) 2011, 2013, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2014 MariaDB Foundation

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "mariadb.h"
#include "item_inetfunc.h"

#include "my_net.h"

///////////////////////////////////////////////////////////////////////////

static const size_t IN_ADDR_SIZE= 4;
static const size_t IN_ADDR_MAX_CHAR_LENGTH= 15;

static const size_t IN6_ADDR_SIZE= 16;
static const size_t IN6_ADDR_NUM_WORDS= IN6_ADDR_SIZE / 2;

/**
  Non-abbreviated syntax is 8 groups, up to 4 digits each,
  plus 7 delimiters between the groups.
  Abbreviated syntax is even shorter.
*/
static const uint IN6_ADDR_MAX_CHAR_LENGTH= 8 * 4 + 7;

static const char HEX_DIGITS[]= "0123456789abcdef";


class NativeBufferInet6: public NativeBuffer<IN6_ADDR_SIZE+1>
{
};

class StringBufferInet6: public StringBuffer<IN6_ADDR_MAX_CHAR_LENGTH+1>
{
};

///////////////////////////////////////////////////////////////////////////

longlong Item_func_inet_aton::val_int()
{
  DBUG_ASSERT(fixed);

  uint byte_result= 0;
  ulonglong result= 0;                    // We are ready for 64 bit addresses
  const char *p,* end;
  char c= '.'; // we mark c to indicate invalid IP in case length is 0
  int dot_count= 0;

  StringBuffer<36> tmp;
  String *s= args[0]->val_str_ascii(&tmp);

  if (!s)       // If null value
    goto err;

  null_value= 0;

  end= (p = s->ptr()) + s->length();
  while (p < end)
  {
    c= *p++;
    int digit= (int) (c - '0');
    if (digit >= 0 && digit <= 9)
    {
      if ((byte_result= byte_result * 10 + digit) > 255)
        goto err;                               // Wrong address
    }
    else if (c == '.')
    {
      dot_count++;
      result= (result << 8) + (ulonglong) byte_result;
      byte_result= 0;
    }
    else
      goto err;                                 // Invalid character
  }
  if (c != '.')                                 // IP number can't end on '.'
  {
    /*
      Attempt to support short forms of IP-addresses. It's however pretty
      basic one comparing to the BSD support.
      Examples:
        127     -> 0.0.0.127
        127.255 -> 127.0.0.255
        127.256 -> NULL (should have been 127.0.1.0)
        127.2.1 -> 127.2.0.1
    */
    switch (dot_count) {
    case 1: result<<= 8; /* Fall through */
    case 2: result<<= 8; /* Fall through */
    }
    return (result << 8) + (ulonglong) byte_result;
  }

err:
  null_value=1;
  return 0;
}


String* Item_func_inet_ntoa::val_str(String* str)
{
  DBUG_ASSERT(fixed);

  ulonglong n= (ulonglong) args[0]->val_int();

  /*
    We do not know if args[0] is NULL until we have called
    some val function on it if args[0] is not a constant!

    Also return null if n > 255.255.255.255
  */
  if ((null_value= (args[0]->null_value || n > 0xffffffff)))
    return 0;                                   // Null value

  str->set_charset(collation.collation);
  str->length(0);

  uchar buf[8];
  int4store(buf, n);

  /* Now we can assume little endian. */

  char num[4];
  num[3]= '.';

  for (uchar *p= buf + 4; p-- > buf;)
  {
    uint c= *p;
    uint n1, n2;                                // Try to avoid divisions
    n1= c / 100;                                // 100 digits
    c-= n1 * 100;
    n2= c / 10;                                 // 10 digits
    c-= n2 * 10;                                // last digit
    num[0]= (char) n1 + '0';
    num[1]= (char) n2 + '0';
    num[2]= (char) c + '0';
    uint length= (n1 ? 4 : n2 ? 3 : 2);         // Remove pre-zero
    uint dot_length= (p <= buf) ? 1 : 0;
    (void) str->append(num + 4 - length, length - dot_length,
                       &my_charset_latin1);
  }

  return str;
}

///////////////////////////////////////////////////////////////////////////

class Inet4
{
  char m_buffer[IN_ADDR_SIZE];
protected:
  bool ascii_to_ipv4(const char *str, size_t length);
  bool character_string_to_ipv4(const char *str, size_t str_length,
                                CHARSET_INFO *cs)
  {
    if (cs->state & MY_CS_NONASCII)
    {
      char tmp[IN_ADDR_MAX_CHAR_LENGTH];
      String_copier copier;
      uint length= copier.well_formed_copy(&my_charset_latin1, tmp, sizeof(tmp),
                                           cs, str, str_length);
      return ascii_to_ipv4(tmp, length);
    }
    return ascii_to_ipv4(str, str_length);
  }
  bool binary_to_ipv4(const char *str, size_t length)
  {
    if (length != sizeof(m_buffer))
      return true;
    memcpy(m_buffer, str, length);
    return false;
  }
  // Non-initializing constructor
  Inet4() { }
public:
  void to_binary(char *dst, size_t dstsize) const
  {
    DBUG_ASSERT(dstsize >= sizeof(m_buffer));
    memcpy(dst, m_buffer, sizeof(m_buffer));
  }
  bool to_binary(String *to) const
  {
    return to->copy(m_buffer, sizeof(m_buffer), &my_charset_bin);
  }
  size_t to_string(char *dst, size_t dstsize) const;
  bool to_string(String *to) const
  {
    to->set_charset(&my_charset_latin1);
    if (to->alloc(INET_ADDRSTRLEN))
      return true;
    to->length((uint32) to_string((char*) to->ptr(), INET_ADDRSTRLEN));
    return false;
  }
};


class Inet4_null: public Inet4, public Null_flag
{
public:
  // Initialize from a text representation
  Inet4_null(const char *str, size_t length, CHARSET_INFO *cs)
   :Null_flag(character_string_to_ipv4(str, length, cs))
  { }
  Inet4_null(const String &str)
   :Inet4_null(str.ptr(), str.length(), str.charset())
  { }
  // Initialize from a binary representation
  Inet4_null(const char *str, size_t length)
   :Null_flag(binary_to_ipv4(str, length))
  { }
  Inet4_null(const Binary_string &str)
   :Inet4_null(str.ptr(), str.length())
  { }
public:
  const Inet4& to_inet4() const
  {
    DBUG_ASSERT(!is_null());
    return *this;
  }
  void to_binary(char *dst, size_t dstsize) const
  {
    to_inet4().to_binary(dst, dstsize);
  }
  bool to_binary(String *to) const
  {
    return to_inet4().to_binary(to);
  }
  size_t to_string(char *dst, size_t dstsize) const
  {
    return to_inet4().to_string(dst, dstsize);
  }
  bool to_string(String *to) const
  {
    return to_inet4().to_string(to);
  }
};


class Inet6
{
protected:
  char m_buffer[IN6_ADDR_SIZE];
  bool make_from_item(Item *item);
  bool ascii_to_ipv6(const char *str, size_t str_length);
  bool character_string_to_ipv6(const char *str, size_t str_length,
                                CHARSET_INFO *cs)
  {
    if (cs->state & MY_CS_NONASCII)
    {
      char tmp[IN6_ADDR_MAX_CHAR_LENGTH];
      String_copier copier;
      uint length= copier.well_formed_copy(&my_charset_latin1, tmp, sizeof(tmp),
                                           cs, str, str_length);
      return ascii_to_ipv6(tmp, length);
    }
    return ascii_to_ipv6(str, str_length);
  }
  bool make_from_character_or_binary_string(const String *str);
  bool binary_to_ipv6(const char *str, size_t length)
  {
    if (length != sizeof(m_buffer))
      return true;
    memcpy(m_buffer, str, length);
    return false;
  }

  Inet6() { }

public:
  static uint binary_length() { return IN6_ADDR_SIZE; }
  /**
    Non-abbreviated syntax is 8 groups, up to 4 digits each,
    plus 7 delimiters between the groups.
    Abbreviated syntax is even shorter.
  */
  static uint max_char_length() { return IN6_ADDR_MAX_CHAR_LENGTH; }

  static bool only_zero_bytes(const char *ptr, uint length)
  {
    for (uint i= 0 ; i < length; i++)
    {
      if (ptr[i] != 0)
        return false;
    }
    return true;
  }

public:

  Inet6(Item *item, bool *error)
  {
    *error= make_from_item(item);
  }
  void to_binary(char *str, size_t str_size) const
  {
    DBUG_ASSERT(str_size >= sizeof(m_buffer));
    memcpy(str, m_buffer, sizeof(m_buffer));
  }
  bool to_binary(String *to) const
  {
    return to->copy(m_buffer, sizeof(m_buffer), &my_charset_bin);
  }
  bool to_native(Native *to) const
  {
    return to->copy(m_buffer, sizeof(m_buffer));
  }
  size_t to_string(char *dst, size_t dstsize) const;
  bool to_string(String *to) const
  {
    to->set_charset(&my_charset_latin1);
    if (to->alloc(INET6_ADDRSTRLEN))
      return true;
    to->length((uint32) to_string((char*) to->ptr(), INET6_ADDRSTRLEN));
    return false;
  }
  bool is_v4compat() const
  {
    static_assert(sizeof(in6_addr) == IN6_ADDR_SIZE, "unexpected in6_addr size");
    return IN6_IS_ADDR_V4COMPAT((struct in6_addr *) m_buffer);
  }
  bool is_v4mapped() const
  {
    static_assert(sizeof(in6_addr) == IN6_ADDR_SIZE, "unexpected in6_addr size");
    return IN6_IS_ADDR_V4MAPPED((struct in6_addr *) m_buffer);
  }
  int cmp(const char *str, size_t length) const
  {
    DBUG_ASSERT(length == sizeof(m_buffer));
    return memcmp(m_buffer, str, length);
  }
  int cmp(const Binary_string &other) const
  {
    return cmp(other.ptr(), other.length());
  }
  int cmp(const Inet6 &other) const
  {
    return memcmp(m_buffer, other.m_buffer, sizeof(m_buffer));
  }
};


class Inet6_zero: public Inet6
{
public:
  Inet6_zero()
  {
    bzero(&m_buffer, sizeof(m_buffer));
  }
};


class Inet6_null: public Inet6, public Null_flag
{
public:
  // Initialize from a text representation
  Inet6_null(const char *str, size_t length, CHARSET_INFO *cs)
   :Null_flag(character_string_to_ipv6(str, length, cs))
  { }
  Inet6_null(const String &str)
   :Inet6_null(str.ptr(), str.length(), str.charset())
  { }
  // Initialize from a binary representation
  Inet6_null(const char *str, size_t length)
   :Null_flag(binary_to_ipv6(str, length))
  { }
  Inet6_null(const Binary_string &str)
   :Inet6_null(str.ptr(), str.length())
  { }
  // Initialize from an Item
  Inet6_null(Item *item)
   :Null_flag(make_from_item(item))
  { }
public:
  const Inet6& to_inet6() const
  {
    DBUG_ASSERT(!is_null());
    return *this;
  }
  void to_binary(char *str, size_t str_size) const
  {
    to_inet6().to_binary(str, str_size);
  }
  bool to_binary(String *to) const
  {
    return to_inet6().to_binary(to);
  }
  size_t to_string(char *dst, size_t dstsize) const
  {
    return to_inet6().to_string(dst, dstsize);
  }
  bool to_string(String *to) const
  {
    return to_inet6().to_string(to);
  }
  bool is_v4compat() const
  {
    return to_inet6().is_v4compat();
  }
  bool is_v4mapped() const
  {
    return to_inet6().is_v4mapped();
  }
};


/**
  Tries to convert given string to binary IPv4-address representation.
  This is a portable alternative to inet_pton(AF_INET).

  @param      str          String to convert.
  @param      str_length   String length.

  @return Completion status.
  @retval true  - error, the given string does not represent an IPv4-address.
  @retval false - ok, the string has been converted sucessfully.

  @note The problem with inet_pton() is that it treats leading zeros in
  IPv4-part differently on different platforms.
*/

bool Inet4::ascii_to_ipv4(const char *str, size_t str_length)
{
  if (str_length < 7)
  {
    DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): "
                         "invalid IPv4 address: too short.",
                         (int) str_length, str));
    return true;
  }

  if (str_length > IN_ADDR_MAX_CHAR_LENGTH)
  {
    DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): "
                         "invalid IPv4 address: too long.",
                         (int) str_length, str));
    return true;
  }

  unsigned char *ipv4_bytes= (unsigned char *) &m_buffer;
  const char *str_end= str + str_length;
  const char *p= str;
  int byte_value= 0;
  int chars_in_group= 0;
  int dot_count= 0;
  char c= 0;

  while (p < str_end && *p)
  {
    c= *p++;

    if (my_isdigit(&my_charset_latin1, c))
    {
      ++chars_in_group;

      if (chars_in_group > 3)
      {
        DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                             "too many characters in a group.",
                             (int) str_length, str));
        return true;
      }

      byte_value= byte_value * 10 + (c - '0');

      if (byte_value > 255)
      {
        DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                             "invalid byte value.",
                             (int) str_length, str));
        return true;
      }
    }
    else if (c == '.')
    {
      if (chars_in_group == 0)
      {
        DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                             "too few characters in a group.",
                             (int) str_length, str));
        return true;
      }

      ipv4_bytes[dot_count]= (unsigned char) byte_value;

      ++dot_count;
      byte_value= 0;
      chars_in_group= 0;

      if (dot_count > 3)
      {
        DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                             "too many dots.", (int) str_length, str));
        return true;
      }
    }
    else
    {
      DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                           "invalid character at pos %d.",
                           (int) str_length, str, (int) (p - str)));
      return true;
    }
  }

  if (c == '.')
  {
    DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                         "ending at '.'.", (int) str_length, str));
    return true;
  }

  if (dot_count != 3)
  {
    DBUG_PRINT("error", ("ascii_to_ipv4(%.*s): invalid IPv4 address: "
                         "too few groups.",
                         (int) str_length, str));
    return true;
  }

  ipv4_bytes[3]= (unsigned char) byte_value;

  DBUG_PRINT("info", ("ascii_to_ipv4(%.*s): valid IPv4 address: %d.%d.%d.%d",
                      (int) str_length, str,
                      ipv4_bytes[0], ipv4_bytes[1],
                      ipv4_bytes[2], ipv4_bytes[3]));
  return false;
}


/**
  Tries to convert given string to binary IPv6-address representation.
  This is a portable alternative to inet_pton(AF_INET6).

  @param      str          String to convert.
  @param      str_length   String length.

  @return Completion status.
  @retval true  - error, the given string does not represent an IPv6-address.
  @retval false - ok, the string has been converted sucessfully.

  @note The problem with inet_pton() is that it treats leading zeros in
  IPv4-part differently on different platforms.
*/

bool Inet6::ascii_to_ipv6(const char *str, size_t str_length)
{
  if (str_length < 2)
  {
    DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: too short.",
                         (int) str_length, str));
    return true;
  }

  if (str_length > IN6_ADDR_MAX_CHAR_LENGTH)
  {
    DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: too long.",
                         (int) str_length, str));
    return true;
  }

  memset(m_buffer, 0, sizeof(m_buffer));

  const char *p= str;

  if (*p == ':')
  {
    ++p;

    if (*p != ':')
    {
      DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                           "can not start with ':x'.", (int) str_length, str));
      return true;
    }
  }

  const char *str_end= str + str_length;
  char *ipv6_bytes_end= m_buffer + sizeof(m_buffer);
  char *dst= m_buffer;
  char *gap_ptr= NULL;
  const char *group_start_ptr= p;
  int chars_in_group= 0;
  int group_value= 0;

  while (p < str_end && *p)
  {
    char c= *p++;

    if (c == ':')
    {
      group_start_ptr= p;

      if (!chars_in_group)
      {
        if (gap_ptr)
        {
          DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                               "too many gaps(::).", (int) str_length, str));
          return true;
        }

        gap_ptr= dst;
        continue;
      }

      if (!*p || p >= str_end)
      {
        DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                             "ending at ':'.", (int) str_length, str));
        return true;
      }

      if (dst + 2 > ipv6_bytes_end)
      {
        DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                             "too many groups (1).", (int) str_length, str));
        return true;
      }

      dst[0]= (unsigned char) (group_value >> 8) & 0xff;
      dst[1]= (unsigned char) group_value & 0xff;
      dst += 2;

      chars_in_group= 0;
      group_value= 0;
    }
    else if (c == '.')
    {
      if (dst + IN_ADDR_SIZE > ipv6_bytes_end)
      {
        DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                             "unexpected IPv4-part.", (int) str_length, str));
        return true;
      }

      Inet4_null tmp(group_start_ptr, (size_t) (str_end - group_start_ptr),
                     &my_charset_latin1);
      if (tmp.is_null())
      {
        DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                             "invalid IPv4-part.", (int) str_length, str));
        return true;
      }

      tmp.to_binary(dst, IN_ADDR_SIZE);
      dst += IN_ADDR_SIZE;
      chars_in_group= 0;

      break;
    }
    else
    {
      const char *hdp= strchr(HEX_DIGITS, my_tolower(&my_charset_latin1, c));

      if (!hdp)
      {
        DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                             "invalid character at pos %d.",
                             (int) str_length, str, (int) (p - str)));
        return true;
      }

      if (chars_in_group >= 4)
      {
        DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                             "too many digits in group.",
                             (int) str_length, str));
        return true;
      }

      group_value <<= 4;
      group_value |= hdp - HEX_DIGITS;

      DBUG_ASSERT(group_value <= 0xffff);

      ++chars_in_group;
    }
  }

  if (chars_in_group > 0)
  {
    if (dst + 2 > ipv6_bytes_end)
    {
      DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                           "too many groups (2).", (int) str_length, str));
      return true;
    }

    dst[0]= (unsigned char) (group_value >> 8) & 0xff;
    dst[1]= (unsigned char) group_value & 0xff;
    dst += 2;
  }

  if (gap_ptr)
  {
    if (dst == ipv6_bytes_end)
    {
      DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                           "no room for a gap (::).", (int) str_length, str));
      return true;
    }

    int bytes_to_move= (int)(dst - gap_ptr);

    for (int i= 1; i <= bytes_to_move; ++i)
    {
      ipv6_bytes_end[-i]= gap_ptr[bytes_to_move - i];
      gap_ptr[bytes_to_move - i]= 0;
    }

    dst= ipv6_bytes_end;
  }

  if (dst < ipv6_bytes_end)
  {
    DBUG_PRINT("error", ("ascii_to_ipv6(%.*s): invalid IPv6 address: "
                         "too few groups.", (int) str_length, str));
    return true;
  }

  return false;
}


/**
  Converts IPv4-binary-address to a string. This function is a portable
  alternative to inet_ntop(AF_INET).

  @param[in] ipv4 IPv4-address data (byte array)
  @param[out] dst A buffer to store string representation of IPv4-address.
  @param[in]  dstsize Number of bytes avaiable in "dst"

  @note The problem with inet_ntop() is that it is available starting from
  Windows Vista, but the minimum supported version is Windows 2000.
*/

size_t Inet4::to_string(char *dst, size_t dstsize) const
{
  return (size_t) my_snprintf(dst, dstsize, "%d.%d.%d.%d",
                              (uchar) m_buffer[0], (uchar) m_buffer[1],
                              (uchar) m_buffer[2], (uchar) m_buffer[3]);
}


/**
  Converts IPv6-binary-address to a string. This function is a portable
  alternative to inet_ntop(AF_INET6).

  @param[in] ipv6 IPv6-address data (byte array)
  @param[out] dst A buffer to store string representation of IPv6-address.
                  It must be at least of INET6_ADDRSTRLEN.
  @param[in] dstsize Number of bytes available dst.

  @note The problem with inet_ntop() is that it is available starting from
  Windows Vista, but out the minimum supported version is Windows 2000.
*/

size_t Inet6::to_string(char *dst, size_t dstsize) const
{
  struct Region
  {
    int pos;
    int length;
  };

  const char *ipv6= m_buffer;
  char *dstend= dst + dstsize;
  const unsigned char *ipv6_bytes= (const unsigned char *) ipv6;

  // 1. Translate IPv6-address bytes to words.
  // We can't just cast to short, because it's not guaranteed
  // that sizeof (short) == 2. So, we have to make a copy.

  uint16 ipv6_words[IN6_ADDR_NUM_WORDS];

  DBUG_ASSERT(dstsize > 0); // Need a space at least for the trailing '\0'
  for (size_t i= 0; i < IN6_ADDR_NUM_WORDS; ++i)
    ipv6_words[i]= (ipv6_bytes[2 * i] << 8) + ipv6_bytes[2 * i + 1];

  // 2. Find "the gap" -- longest sequence of zeros in IPv6-address.

  Region gap= { -1, -1 };

  {
    Region rg= { -1, -1 };

    for (size_t i= 0; i < IN6_ADDR_NUM_WORDS; ++i)
    {
      if (ipv6_words[i] != 0)
      {
        if (rg.pos >= 0)
        {
          if (rg.length > gap.length)
            gap= rg;

          rg.pos= -1;
          rg.length= -1;
        }
      }
      else
      {
        if (rg.pos >= 0)
        {
          ++rg.length;
        }
        else
        {
          rg.pos= (int) i;
          rg.length= 1;
        }
      }
    }

    if (rg.pos >= 0)
    {
      if (rg.length > gap.length)
        gap= rg;
    }
  }

  // 3. Convert binary data to string.

  char *p= dst;

  for (int i= 0; i < (int) IN6_ADDR_NUM_WORDS; ++i)
  {
    DBUG_ASSERT(dstend >= p);
    size_t dstsize_available= dstend - p;
    if (dstsize_available < 5)
      break;
    if (i == gap.pos)
    {
      // We're at the gap position. We should put trailing ':' and jump to
      // the end of the gap.

      if (i == 0)
      {
        // The gap starts from the beginning of the data -- leading ':'
        // should be put additionally.

        *p= ':';
        ++p;
      }

      *p= ':';
      ++p;

      i += gap.length - 1;
    }
    else if (i == 6 && gap.pos == 0 &&
             (gap.length == 6 ||                           // IPv4-compatible
              (gap.length == 5 && ipv6_words[5] == 0xffff) // IPv4-mapped
             ))
    {
      // The data represents either IPv4-compatible or IPv4-mapped address.
      // The IPv6-part (zeros or zeros + ffff) has been already put into
      // the string (dst). Now it's time to dump IPv4-part.

      return (size_t) (p - dst) +
             Inet4_null((const char *) (ipv6_bytes + 12), 4).
               to_string(p, dstsize_available);
    }
    else
    {
      // Usual IPv6-address-field. Print it out using lower-case
      // hex-letters without leading zeros (recommended IPv6-format).
      //
      // If it is not the last field, append closing ':'.

      p += sprintf(p, "%x", ipv6_words[i]);

      if (i + 1 != IN6_ADDR_NUM_WORDS)
      {
        *p= ':';
        ++p;
      }
    }
  }

  *p= 0;
  return (size_t) (p - dst);
}

///////////////////////////////////////////////////////////////////////////

/**
  Converts IP-address-string to IP-address-data.

    ipv4-string -> varbinary(4)
    ipv6-string -> varbinary(16)

  @return Completion status.
  @retval NULL  Given string does not represent an IP-address.
  @retval !NULL The string has been converted sucessfully.
*/

String *Item_func_inet6_aton::val_str(String *buffer)
{
  DBUG_ASSERT(fixed);

  Ascii_ptr_and_buffer<STRING_BUFFER_USUAL_SIZE> tmp(args[0]);
  if ((null_value= tmp.is_null()))
    return NULL;

  Inet4_null ipv4(*tmp.string());
  if (!ipv4.is_null())
  {
    ipv4.to_binary(buffer);
    return buffer;
  }

  Inet6_null ipv6(*tmp.string());
  if (!ipv6.is_null())
  {
    ipv6.to_binary(buffer);
    return buffer;
  }

  null_value= true;
  return NULL;
}


/**
  Converts IP-address-data to IP-address-string.
*/

String *Item_func_inet6_ntoa::val_str_ascii(String *buffer)
{
  DBUG_ASSERT(fixed);

  // Binary string argument expected
  if (unlikely(args[0]->result_type() != STRING_RESULT ||
               args[0]->collation.collation != &my_charset_bin))
  {
    null_value= true;
    return NULL;
  }

  String_ptr_and_buffer<STRING_BUFFER_USUAL_SIZE> tmp(args[0]);
  if ((null_value= tmp.is_null()))
    return NULL;

  Inet4_null ipv4(static_cast<const Binary_string&>(*tmp.string()));
  if (!ipv4.is_null())
  {
    ipv4.to_string(buffer);
    return buffer;
  }

  Inet6_null ipv6(static_cast<const Binary_string&>(*tmp.string()));
  if (!ipv6.is_null())
  {
    ipv6.to_string(buffer);
    return buffer;
  }

  DBUG_PRINT("info", ("INET6_NTOA(): varbinary(4) or varbinary(16) expected."));
  null_value= true;
  return NULL;
}


/**
  Checks if the passed string represents an IPv4-address.
*/

longlong Item_func_is_ipv4::val_int()
{
  DBUG_ASSERT(fixed);
  String_ptr_and_buffer<STRING_BUFFER_USUAL_SIZE> tmp(args[0]);
  return !tmp.is_null() && !Inet4_null(*tmp.string()).is_null();
}


/**
  Checks if the passed string represents an IPv6-address.
*/

longlong Item_func_is_ipv6::val_int()
{
  DBUG_ASSERT(fixed);
  String_ptr_and_buffer<STRING_BUFFER_USUAL_SIZE> tmp(args[0]);
  return !tmp.is_null() && !Inet6_null(*tmp.string()).is_null();
}


/**
  Checks if the passed IPv6-address is an IPv4-compat IPv6-address.
*/

longlong Item_func_is_ipv4_compat::val_int()
{
  Inet6_null ip6(args[0]);
  return !ip6.is_null() && ip6.is_v4compat();
}


/**
  Checks if the passed IPv6-address is an IPv4-mapped IPv6-address.
*/

longlong Item_func_is_ipv4_mapped::val_int()
{
  Inet6_null ip6(args[0]);
  return !ip6.is_null() && ip6.is_v4mapped();
}


/********************************************************************/
#include "sql_class.h" // SORT_FIELD_ATTR
#include "opt_range.h" // SEL_ARG

extern SEL_ARG null_element;

class Type_std_attributes_inet6: public Type_std_attributes
{
public:
  Type_std_attributes_inet6()
   :Type_std_attributes(Inet6::max_char_length(), 0, true,
                        DTCollation(&my_charset_numeric,
                                    DERIVATION_NUMERIC,
                                    MY_REPERTOIRE_ASCII))
  { }
};


class cmp_item_inet6: public cmp_item_scalar
{
  Inet6 m_native;
public:
  cmp_item_inet6()
   :cmp_item_scalar(),
    m_native(Inet6_zero())
  { }
  void store_value(Item *item) override
  {
    m_native= Inet6(item, &m_null_value);
  }
  int cmp_not_null(const Value *val) override
  {
    DBUG_ASSERT(!val->is_null());
    DBUG_ASSERT(val->is_string());
    Inet6_null tmp(val->m_string);
    DBUG_ASSERT(!tmp.is_null());
    return m_native.cmp(tmp);
  }
  int cmp(Item *arg) override
  {
    Inet6_null tmp(arg);
    return m_null_value || tmp.is_null() ? UNKNOWN : m_native.cmp(tmp) != 0;
  }
  int compare(cmp_item *ci) override
  {
    cmp_item_inet6 *tmp= static_cast<cmp_item_inet6*>(ci);
    DBUG_ASSERT(!m_null_value);
    DBUG_ASSERT(!tmp->m_null_value);
    return m_native.cmp(tmp->m_native);
  }
  cmp_item *make_same() override
  {
    return new cmp_item_inet6();
  }
};


class Type_handler_inet6: public Type_handler
{
  static const Name m_name_inet6;

  bool character_or_binary_string_to_native(THD *thd, const String *str,
                                            Native *to) const
  {
    if (str->charset() == &my_charset_bin)
    {
      // Convert from a binary string
      if (str->length() != Inet6::binary_length() ||
          to->copy(str->ptr(), str->length()))
      {
        thd->push_warning_wrong_value(Sql_condition::WARN_LEVEL_WARN,
                                      m_name_inet6.ptr(),
                                      ErrConvString(str).ptr());
        return true;
      }
      return false;
    }
    // Convert from a character string
    Inet6_null tmp(*str);
    if (tmp.is_null())
      thd->push_warning_wrong_value(Sql_condition::WARN_LEVEL_WARN,
                                    m_name_inet6.ptr(),
                                    ErrConvString(str).ptr());
    return tmp.is_null() || tmp.to_native(to);
  }

public:
  ~Type_handler_inet6() override {}

  const Name name() const override { return m_name_inet6; }
  const Name version() const override { return m_version_default; }
  protocol_send_type_t protocol_send_type() const override
  {
    return PROTOCOL_SEND_STRING;
  }

  enum_field_types field_type() const override
  {
    return MYSQL_TYPE_STRING;
  }

  enum_field_types real_field_type() const override
  {
    return (enum_field_types) 128;
  }

  Item_result result_type() const override
  {
    return STRING_RESULT;
  }

  Item_result cmp_type() const override
  {
    return STRING_RESULT;
  }

  const Type_handler *type_handler_for_comparison() const override
  {
    return this;
  }

  int
  stored_field_cmp_to_item(THD *thd, Field *field, Item *item) const override
  {
    DBUG_ASSERT(field->type_handler() == this);
    Inet6_null ni(item); // Convert Item to INET6
    if (ni.is_null())
      return 0;
    NativeBufferInet6 tmp;
    if (field->val_native(&tmp))
    {
      DBUG_ASSERT(0);
      return 0;
    }
    return -ni.cmp(tmp);
  }
  CHARSET_INFO *charset_for_protocol(const Item *item) const override
  {
    return item->collation.collation;
  }

  bool is_traditional_type() const override
  {
    return false;
  }
  bool is_scalar_type() const override { return true; }
  bool can_return_int() const override { return false; }
  bool can_return_decimal() const override { return false; }
  bool can_return_real() const override { return false; }
  bool can_return_str() const override { return true; }
  bool can_return_text() const override { return true; }
  bool can_return_date() const override { return false; }
  bool can_return_time() const override { return false; }

  uint Item_time_precision(THD *thd, Item *item) const override
  {
    return 0;
  }
  uint Item_datetime_precision(THD *thd, Item *item) const override
  {
    return 0;
  }
  uint Item_decimal_scale(const Item *item) const override
  {
    return 0;
  }
  uint Item_decimal_precision(const Item *item) const override
  {
    /*
      This will be needed if we ever allow cast from INET6 to DECIMAL.
      Decimal precision of INET6 is 39 digits:
      'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' =
       340282366920938463463374607431768211456  = 39 digits
    */
    return 39;
  }

  /*
    Returns how many digits a divisor adds into a division result.
    See Item::divisor_precision_increment() in item.h for more comments.
  */
  uint Item_divisor_precision_increment(const Item *) const override
  {
    return 0;
  }
  /**
    Makes a temporary table Field to handle numeric aggregate functions,
    e.g. SUM(DISTINCT expr), AVG(DISTINCT expr), etc.
  */
  Field *make_num_distinct_aggregator_field(MEM_ROOT *,
                                            const Item *) const override
  {
    DBUG_ASSERT(0);
    return 0;
  }
  Field *make_conversion_table_field(TABLE *TABLE,
                                     uint metadata,
                                     const Field *target) const override;
  // Fix attributes after the parser
  bool Column_definition_fix_attributes(Column_definition *c) const override
  {
    c->length= Inet6::max_char_length();
    return false;
  }

  bool Column_definition_prepare_stage1(THD *thd,
                                        MEM_ROOT *mem_root,
                                        Column_definition *def,
                                        handler *file,
                                        ulonglong table_flags) const override
  {
    def->create_length_to_internal_length_simple();
    return false;
  }

  bool Column_definition_redefine_stage1(Column_definition *def,
                                         const Column_definition *dup,
                                         const handler *file,
                                         const Schema_specification_st *schema)
                                         const override
  {
    def->redefine_stage1_common(dup, file, schema);
    def->set_compression_method(dup->compression_method());
    def->create_length_to_internal_length_string();
    return false;
  }

  bool Column_definition_prepare_stage2(Column_definition *def,
                                        handler *file,
                                        ulonglong table_flags) const override
  {
    def->pack_flag= FIELDFLAG_BINARY;
    return false;
  }

  Field *make_table_field(const LEX_CSTRING *name,
                          const Record_addr &addr,
                          const Type_all_attributes &attr,
                          TABLE *table) const override;

  Field *
  make_table_field_from_def(TABLE_SHARE *share,
                            MEM_ROOT *mem_root,
                            const LEX_CSTRING *name,
                            const Record_addr &addr,
                            const Bit_addr &bit,
                            const Column_definition_attributes *attr,
                            uint32 flags) const override;
  void
  Column_definition_attributes_frm_pack(const Column_definition_attributes *def,
                                        uchar *buff) const override
  {
    def->frm_pack_basic(buff);
    def->frm_pack_charset(buff);
  }
  bool
  Column_definition_attributes_frm_unpack(Column_definition_attributes *def,
                                          TABLE_SHARE *share,
                                          const uchar *buffer,
                                          LEX_CUSTRING *gis_options)
                                          const override
  {
    def->frm_unpack_basic(buffer);
    return def->frm_unpack_charset(share, buffer);
  }
  void make_sort_key(uchar *to, Item *item,
                     const SORT_FIELD_ATTR *sort_field, Sort_param *param)
                     const override
  {
    DBUG_ASSERT(item->type_handler() == this);
    NativeBufferInet6 tmp;
    item->val_native_result(current_thd, &tmp);
    if (item->maybe_null)
    {
      if (item->null_value)
      {
        memset(to, 0, Inet6::binary_length() + 1);
        return;
      }
      *to++= 1;
    }
    DBUG_ASSERT(!item->null_value);
    DBUG_ASSERT(Inet6::binary_length() == tmp.length());
    DBUG_ASSERT(Inet6::binary_length() == sort_field->length);
    memcpy(to, tmp.ptr(), tmp.length());
  }
  void sortlength(THD *thd,
                  const Type_std_attributes *item,
                  SORT_FIELD_ATTR *attr) const override
  {
    attr->length= Inet6::binary_length();
    attr->suffix_length= 0;
  }
  uint32 max_display_length(const Item *item) const override
  {
    return Inet6::max_char_length();
  }
  uint32 calc_pack_length(uint32 length) const override
  {
    return Inet6::binary_length();
  }
  void Item_update_null_value(Item *item) const override
  {
    NativeBufferInet6 tmp;
    item->val_native(current_thd, &tmp);
  }
  bool Item_save_in_value(THD *thd, Item *item, st_value *value) const override
  {
    value->m_type= DYN_COL_STRING;
    String *str= item->val_str(&value->m_string);
    if (str != &value->m_string && !item->null_value)
    {
      // "item" returned a non-NULL value
      if (Inet6_null(*str).is_null())
      {
        /*
          The value was not-null, but conversion to INET6 failed:
            SELECT a, DECODE_ORACLE(inet6col, 'garbage', '<NULL>', '::01', '01')
            FROM t1;
        */
        thd->push_warning_wrong_value(Sql_condition::WARN_LEVEL_WARN,
                                      m_name_inet6.ptr(),
                                      ErrConvString(str).ptr());
        value->m_type= DYN_COL_NULL;
        return true;
      }
      // "item" returned a non-NULL value, and it was a valid INET6
      value->m_string.set(str->ptr(), str->length(), str->charset());
    }
    return check_null(item, value);
  }
  void Item_param_setup_conversion(THD *thd, Item_param *param) const override
  {
    param->setup_conversion_string(thd, thd->variables.character_set_client);
  }
  void Item_param_set_param_func(Item_param *param,
                                 uchar **pos, ulong len) const override
  {
    param->set_param_str(pos, len);
  }
  bool Item_param_set_from_value(THD *thd,
                                 Item_param *param,
                                 const Type_all_attributes *attr,
                                 const st_value *val) const override
  {
    param->unsigned_flag= false;//QQ
    param->setup_conversion_string(thd, attr->collation.collation);
    /*
      Exact value of max_length is not known unless data is converted to
      charset of connection, so we have to set it later.
    */
    return param->set_str(val->m_string.ptr(), val->m_string.length(),
                          attr->collation.collation,
                          attr->collation.collation);
  }
  bool Item_param_val_native(THD *thd, Item_param *item, Native *to)
                             const override
  {
    StringBufferInet6 buffer;
    String *str= item->val_str(&buffer);
    if (!str)
      return true;
    Inet6_null tmp(*str);
    return tmp.is_null() || tmp.to_native(to);
  }
  bool Item_send(Item *item, Protocol *p, st_value *buf) const override
  {
    return Item_send_str(item, p, buf);
  }
  int Item_save_in_field(Item *item, Field *field, bool no_conversions)
                         const override
  {
    if (field->type_handler() == this)
    {
      NativeBuffer<MAX_FIELD_WIDTH> tmp;
      bool rc= item->val_native(current_thd, &tmp);
      if (rc || item->null_value)
        return set_field_to_null_with_conversions(field, no_conversions);
      field->set_notnull();
      return field->store_native(tmp);
    }
    return item->save_str_in_field(field, no_conversions);
  }

  String *print_item_value(THD *thd, Item *item, String *str) const override
  {
    StringBufferInet6 buf;
    String *result= item->val_str(&buf);
    return !result ||
           str->realloc(name().length() + result->length() + 2) ||
           str->copy(name().ptr(), name().length(), &my_charset_latin1) ||
           str->append('\'') ||
           str->append(result->ptr(), result->length()) ||
           str->append('\'') ?
           NULL :
           str;
  }

  /**
    Check if
      WHERE expr=value AND expr=const
    can be rewritten as:
      WHERE const=value AND expr=const

    "this" is the comparison handler that is used by "target".

    @param target       - the predicate expr=value,
                          whose "expr" argument will be replaced to "const".
    @param target_expr  - the target's "expr" which will be replaced to "const".
    @param target_value - the target's second argument, it will remain unchanged.
    @param source       - the equality predicate expr=const (or expr<=>const)
                          that can be used to rewrite the "target" part
                          (under certain conditions, see the code).
    @param source_expr  - the source's "expr". It should be exactly equal to
                          the target's "expr" to make condition rewrite possible.
    @param source_const - the source's "const" argument, it will be inserted
                          into "target" instead of "expr".
  */
  bool
  can_change_cond_ref_to_const(Item_bool_func2 *target,
                               Item *target_expr, Item *target_value,
                               Item_bool_func2 *source,
                               Item *source_expr, Item *source_const)
                               const override
  {
    /*
      WHERE COALESCE(inet6_col)='::1' AND COALESCE(inet6_col)=CONCAT(a);  -->
      WHERE COALESCE(inet6_col)='::1' AND               '::1'=CONCAT(a);
    */
    return target->compare_type_handler() == source->compare_type_handler();
  }
  bool
  subquery_type_allows_materialization(const Item *inner,
                                       const Item *outer) const override
  {
    /*
      Example:
        SELECT * FROM t1 WHERE a IN (SELECT inet6col FROM t1 GROUP BY inet6col);
      Allow materialization only if the outer column is also INET6.
      This can be changed for more relaxed rules in the future.
    */
    DBUG_ASSERT(inner->type_handler() == this);
    return outer->type_handler() == this;
  }
  /**
    Make a simple constant replacement item for a constant "src",
    so the new item can futher be used for comparison with "cmp", e.g.:
      src = cmp   ->  replacement = cmp

    "this" is the type handler that is used to compare "src" and "cmp".

    @param thd - current thread, for mem_root
    @param src - The item that we want to replace. It's a const item,
                 but it can be complex enough to calculate on every row.
    @param cmp - The src's comparand.
    @retval    - a pointer to the created replacement Item
    @retval    - NULL, if could not create a replacement (e.g. on EOM).
                 NULL is also returned for ROWs, because instead of replacing
                 a Item_row to a new Item_row, Type_handler_row just replaces
                 its elements.
  */
  Item *make_const_item_for_comparison(THD *thd,
                                       Item *src,
                                       const Item *cmp) const override;
  Item_cache *Item_get_cache(THD *thd, const Item *item) const override;

  Item *create_typecast_item(THD *thd, Item *item,
                             const Type_cast_attributes &attr) const override;

  int cmp_native(const Native &a, const Native &b) const override
  {
    DBUG_ASSERT(a.length() == Inet6::binary_length());
    DBUG_ASSERT(b.length() == Inet6::binary_length());
    return memcmp(a.ptr(), b.ptr(), Inet6::binary_length());
  }
  bool set_comparator_func(Arg_comparator *cmp) const override
  {
    return cmp->set_cmp_func_native();
  }
  bool Item_const_eq(const Item_const *a, const Item_const *b,
                             bool binary_cmp) const override
  {
    return false;//QQ
  }
  bool Item_eq_value(THD *thd, const Type_cmp_attributes *attr,
                     Item *a, Item *b) const override
  {
    Inet6_null na(a);
    Inet6_null nb(b);
    return !na.is_null() && !nb.is_null() && !na.cmp(nb);
  }
  bool Item_hybrid_func_fix_attributes(THD *thd,
                                       const char *name,
                                       Type_handler_hybrid_field_type *h,
                                       Type_all_attributes *attr,
                                       Item **items,
                                       uint nitems) const override
  {
    attr->Type_std_attributes::operator=(Type_std_attributes_inet6());
    h->set_handler(this);
    return false;
  }
  bool Item_func_min_max_fix_attributes(THD *thd,
                                        Item_func_min_max *func,
                                        Item **items,
                                        uint nitems) const override
  {
    return Item_hybrid_func_fix_attributes(thd, func->func_name(),
                                           func, func, items, nitems);

  }
  bool Item_sum_hybrid_fix_length_and_dec(Item_sum_hybrid *func) const override
  {
    func->Type_std_attributes::operator=(Type_std_attributes_inet6());
    func->set_handler(this);
    return false;
  }
  bool Item_sum_sum_fix_length_and_dec(Item_sum_sum *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }
  bool Item_sum_avg_fix_length_and_dec(Item_sum_avg *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }
  bool Item_sum_variance_fix_length_and_dec(Item_sum_variance *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }

  bool Item_val_native_with_conversion(THD *thd, Item *item,
                                       Native *to) const override
  {
    if (item->type_handler() == this)
      return item->val_native(thd, to); // No conversion needed
    StringBufferInet6 buffer;
    String *str= item->val_str(&buffer);
    return str ? character_or_binary_string_to_native(thd, str, to) : true;
  }
  bool Item_val_native_with_conversion_result(THD *thd, Item *item,
                                              Native *to) const override
  {
    if (item->type_handler() == this)
      return item->val_native_result(thd, to); // No conversion needed
    StringBufferInet6 buffer;
    String *str= item->str_result(&buffer);
    return str ? character_or_binary_string_to_native(thd, str, to) : true;
  }

  bool Item_val_bool(Item *item) const override
  {
    NativeBufferInet6 tmp;
    if (item->val_native(current_thd, &tmp))
      return false;
    return !Inet6::only_zero_bytes(tmp.ptr(), tmp.length());
  }
  void Item_get_date(THD *thd, Item *item,
                     Temporal::Warn *buff, MYSQL_TIME *ltime,
                     date_mode_t fuzzydate) const override
  {
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
  }

  longlong Item_val_int_signed_typecast(Item *item) const override
  {
    DBUG_ASSERT(0);
    return 0;
  }

  longlong Item_val_int_unsigned_typecast(Item *item) const override
  {
    DBUG_ASSERT(0);
    return 0;
  }

  String *Item_func_hex_val_str_ascii(Item_func_hex *item, String *str)
                                      const override
  {
    NativeBufferInet6 tmp;
    if ((item->null_value= item->arguments()[0]->val_native(current_thd, &tmp)))
      return NULL;
    DBUG_ASSERT(tmp.length() == Inet6::binary_length());
    if (str->set_hex(tmp.ptr(), tmp.length()))
    {
      str->length(0);
      str->set_charset(item->collation.collation);
    }
    return str;
  }

  String *Item_func_hybrid_field_type_val_str(Item_func_hybrid_field_type *item,
                                              String *str) const override
  {
    NativeBufferInet6 native;
    if (item->val_native(current_thd, &native))
    {
      DBUG_ASSERT(item->null_value);
      return NULL;
    }
    DBUG_ASSERT(native.length() == Inet6::binary_length());
    Inet6_null tmp(native.ptr(), native.length());
    return tmp.is_null() || tmp.to_string(str) ? NULL : str;
  }
  double Item_func_hybrid_field_type_val_real(Item_func_hybrid_field_type *)
                                              const override
  {
    return 0;
  }
  longlong Item_func_hybrid_field_type_val_int(Item_func_hybrid_field_type *)
                                               const override
  {
    return 0;
  }
  my_decimal *
  Item_func_hybrid_field_type_val_decimal(Item_func_hybrid_field_type *,
                                          my_decimal *to) const override
  {
    my_decimal_set_zero(to);
    return to;
  }
  void Item_func_hybrid_field_type_get_date(THD *,
                                            Item_func_hybrid_field_type *,
                                            Temporal::Warn *,
                                            MYSQL_TIME *to,
                                            date_mode_t fuzzydate)
                                            const override
  {
    set_zero_time(to, MYSQL_TIMESTAMP_TIME);
  }
  // WHERE is Item_func_min_max_val_native???
  String *Item_func_min_max_val_str(Item_func_min_max *func, String *str)
                                    const override
  {
    Inet6_null tmp(func);
    return tmp.is_null() || tmp.to_string(str) ? NULL : str;
  }
  double Item_func_min_max_val_real(Item_func_min_max *) const override
  {
    return 0;
  }
  longlong Item_func_min_max_val_int(Item_func_min_max *) const override
  {
    return 0;
  }
  my_decimal *Item_func_min_max_val_decimal(Item_func_min_max *,
                                            my_decimal *to) const override
  {
    my_decimal_set_zero(to);
    return to;
  }
  bool Item_func_min_max_get_date(THD *thd, Item_func_min_max*,
                                  MYSQL_TIME *to, date_mode_t fuzzydate)
                                  const override
  {
    set_zero_time(to, MYSQL_TIMESTAMP_TIME);
    return false;
  }

  bool
  Item_func_between_fix_length_and_dec(Item_func_between *func) const override
  {
    return false;
  }
  longlong Item_func_between_val_int(Item_func_between *func) const override
  {
    return func->val_int_cmp_native();
  }

  cmp_item *make_cmp_item(THD *thd, CHARSET_INFO *cs) const override
  {
    return new (thd->mem_root) cmp_item_inet6;
  }

  in_vector *make_in_vector(THD *thd, const Item_func_in *func,
                            uint nargs) const override;

  bool Item_func_in_fix_comparator_compatible_types(THD *thd,
                                                    Item_func_in *func)
                                                    const override
  {
    if (func->compatible_types_scalar_bisection_possible())
    {
      return func->value_list_convert_const_to_int(thd) ||
             func->fix_for_scalar_comparison_using_bisection(thd);
    }
    return
      func->fix_for_scalar_comparison_using_cmp_items(thd,
                                                      1U << (uint) STRING_RESULT);
  }
  bool
  Item_func_round_fix_length_and_dec(Item_func_round *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }
  bool
  Item_func_int_val_fix_length_and_dec(Item_func_int_val *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }

  bool Item_func_abs_fix_length_and_dec(Item_func_abs *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }

  bool Item_func_neg_fix_length_and_dec(Item_func_neg *func) const override
  {
    return Item_func_or_sum_illegal_param(func);
  }

  bool
  Item_func_signed_fix_length_and_dec(Item_func_signed *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_func_unsigned_fix_length_and_dec(Item_func_unsigned *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_double_typecast_fix_length_and_dec(Item_double_typecast *item)
                                          const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_decimal_typecast_fix_length_and_dec(Item_decimal_typecast *item)
                                           const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_char_typecast_fix_length_and_dec(Item_char_typecast *item)
                                        const override
  {
    item->fix_length_and_dec_str();
    return false;
  }
  bool
  Item_time_typecast_fix_length_and_dec(Item_time_typecast *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_date_typecast_fix_length_and_dec(Item_date_typecast *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_datetime_typecast_fix_length_and_dec(Item_datetime_typecast *item)
                                            const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_func_plus_fix_length_and_dec(Item_func_plus *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_func_minus_fix_length_and_dec(Item_func_minus *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_func_mul_fix_length_and_dec(Item_func_mul *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_func_div_fix_length_and_dec(Item_func_div *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Item_func_mod_fix_length_and_dec(Item_func_mod *item) const override
  {
    return Item_func_or_sum_illegal_param(item);
  }
  bool
  Vers_history_point_resolve_unit(THD *thd, Vers_history_point *point)
                                  const override
  {
    point->bad_expression_data_type_error(name().ptr());
    return true;
  }
};


const Name Type_handler_inet6::m_name_inet6(STRING_WITH_LEN("inet6"));

Type_handler_inet6 type_handler_inet6;


bool Inet6::make_from_item(Item *item)
{
  if (item->type_handler() == &type_handler_inet6)
  {
    Native tmp(m_buffer, sizeof(m_buffer));
    bool rc= item->val_native(current_thd, &tmp);
    if (rc)
      return true;
    DBUG_ASSERT(tmp.length() == sizeof(m_buffer));
    if (tmp.ptr() != m_buffer)
      memcpy(m_buffer, tmp.ptr(), sizeof(m_buffer));
    return false;
  }
  StringBufferInet6 tmp;
  String *str= item->val_str(&tmp);
  return str ? make_from_character_or_binary_string(str) : true;
}


bool Inet6::make_from_character_or_binary_string(const String *str)
{
  static Name name= type_handler_inet6.name();
  if (str->charset() != &my_charset_bin)
  {
    bool rc= character_string_to_ipv6(str->ptr(), str->length(),
                                      str->charset());
    if (rc)
      current_thd->push_warning_wrong_value(Sql_condition::WARN_LEVEL_WARN,
                                            name.ptr(),
                                            ErrConvString(str).ptr());
    return rc;
  }
  if (str->length() != sizeof(m_buffer))
  {
    current_thd->push_warning_wrong_value(Sql_condition::WARN_LEVEL_WARN,
                                          name.ptr(),
                                          ErrConvString(str).ptr());
    return true;
  }
  DBUG_ASSERT(str->ptr() != m_buffer);
  memcpy(m_buffer, str->ptr(), sizeof(m_buffer));
  return false;
};


class Field_inet6: public Field
{
  static void set_min_value(char *ptr)
  {
    memset(ptr, 0, Inet6::binary_length());
  }
  static void set_max_value(char *ptr)
  {
    memset(ptr, 0xFF, Inet6::binary_length());
  }
  void store_warning(const ErrConv &str,
                     Sql_condition::enum_warning_level level)
  {
    static const Name type_name= type_handler_inet6.name();
    get_thd()->push_warning_truncated_value_for_field(level, type_name.ptr(),
                                                      str.ptr(), table->s,
                                                      field_name.str);
  }
  int set_null_with_warn(const ErrConv &str)
  {
    store_warning(str, Sql_condition::WARN_LEVEL_WARN);
    set_null();
    return 1;
  }
  int set_min_value_with_warn(const ErrConv &str)
  {
    store_warning(str, Sql_condition::WARN_LEVEL_WARN);
    set_min_value((char*) ptr);
    return 1;
  }
  int set_max_value_with_warn(const ErrConv &str)
  {
    store_warning(str, Sql_condition::WARN_LEVEL_WARN);
    set_max_value((char*) ptr);
    return 1;
  }

public:
  Field_inet6(const LEX_CSTRING *field_name_arg, const Record_addr &rec)
    :Field(rec.ptr(), Inet6::max_char_length(),
           rec.null_ptr(), rec.null_bit(), Field::NONE, field_name_arg)
  {
    flags|= BINARY_FLAG | UNSIGNED_FLAG;
  }
  enum_field_types type() const override
  {
    return type_handler_inet6.field_type();
  }
  const Type_handler *type_handler() const override
  {
    return &type_handler_inet6;
  }
  uint32 max_display_length() const override { return field_length; }
  bool str_needs_quotes() override { return true; }
  enum Derivation derivation(void) const override { return DERIVATION_NUMERIC; }
  uint repertoire(void) const override { return MY_REPERTOIRE_ASCII; }
  CHARSET_INFO *charset(void) const override { return &my_charset_numeric; }
  const CHARSET_INFO *sort_charset(void) const override { return &my_charset_bin; }
  /**
    This makes client-server protocol convert the value according
    to @@character_set_client.
  */
  bool binary() const override { return false; }
  enum ha_base_keytype key_type() const override { return HA_KEYTYPE_BINARY; }

  uint is_equal(Create_field *new_field) override
  {
    return new_field->type_handler() == type_handler();
  }
  bool eq_def(const Field *field) const override
  {
    return Field::eq_def(field);
  }
  double pos_in_interval(Field *min, Field *max) override
  {
    return pos_in_interval_val_str(min, max, 0);
  }
  int cmp(const uchar *a, const uchar *b) override
  { return memcmp(a, b, pack_length()); }

  void sort_string(uchar *to, uint length) override
  {
    DBUG_ASSERT(length == pack_length());
    memcpy(to, ptr, length);
  }
  uint32 pack_length() const override
  {
    return Inet6::binary_length();
  }

  void sql_type(String &str) const override
  {
    static Name name= type_handler_inet6.name();
    str.set_ascii(name.ptr(), name.length());
  }

  bool validate_value_in_record(THD *thd, const uchar *record) const override
  {
    return false;
  }

  String *val_str(String *val_buffer,
                  String *val_ptr __attribute__((unused))) override
  {
    DBUG_ASSERT(marked_for_read());
    Inet6_null tmp((const char *) ptr, pack_length());
    return tmp.to_string(val_buffer) ? NULL : val_buffer;
  }

  my_decimal *val_decimal(my_decimal *to) override
  {
    DBUG_ASSERT(marked_for_read());
    my_decimal_set_zero(to);
    return to;
  }

  longlong val_int() override
  {
    DBUG_ASSERT(marked_for_read());
    return 0;
  }

  double val_real() override
  {
    DBUG_ASSERT(marked_for_read());
    return 0;
  }

  bool get_date(MYSQL_TIME *ltime, date_mode_t fuzzydate) override
  {
    DBUG_ASSERT(marked_for_read());
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
    return false;
  }

  bool val_bool(void) override
  {
    DBUG_ASSERT(marked_for_read());
    return !Inet6::only_zero_bytes((const char *) ptr, Inet6::binary_length());
  }

  int store_native(const Native &value) override
  {
    DBUG_ASSERT(marked_for_write_or_computed());
    DBUG_ASSERT(value.length() == Inet6::binary_length());
    memcpy(ptr, value.ptr(), value.length());
    return 0;
  }

  int store(const char *str, size_t length, CHARSET_INFO *cs) override
  {
    DBUG_ASSERT(marked_for_write_or_computed());
    Inet6_null tmp= cs == &my_charset_bin ?
                    Inet6_null(str, length) :
                    Inet6_null(str, length, cs);
    if (tmp.is_null())
    {
      return maybe_null() ?
             set_null_with_warn(ErrConvString(str, length, cs)) :
             set_min_value_with_warn(ErrConvString(str, length, cs));
    }
    tmp.to_binary((char *) ptr, Inet6::binary_length());
    return 0;
  }

  int store_hex_hybrid(const char *str, size_t length) override
  {
    return store(str, length, &my_charset_bin);
  }

  int store_decimal(const my_decimal *num) override
  {
    DBUG_ASSERT(marked_for_write_or_computed());
    return set_min_value_with_warn(ErrConvDecimal(num));
  }

  int store(longlong nr, bool unsigned_flag) override
  {
    DBUG_ASSERT(marked_for_write_or_computed());
    return set_min_value_with_warn(
            ErrConvInteger(Longlong_hybrid(nr, unsigned_flag)));
  }

  int store(double nr) override
  {
    DBUG_ASSERT(marked_for_write_or_computed());
    return set_min_value_with_warn(ErrConvDouble(nr));
  }

  int store_time_dec(const MYSQL_TIME *ltime, uint dec) override
  {
    DBUG_ASSERT(marked_for_write_or_computed());
    return set_min_value_with_warn(ErrConvTime(ltime));
  }

  /*** Field conversion routines ***/
  int store_field(Field *from) override
  {
    // INSERT INTO t1 (inet6_field) SELECT different_field_type FROM t2;
    return from->save_in_field(this);
  }
  int save_in_field(Field *to) override
  {
    // INSERT INTO t2 (different_field_type) SELECT inet6_field FROM t1;
    switch (to->cmp_type()) {
    case INT_RESULT:
    case REAL_RESULT:
    case DECIMAL_RESULT:
    case TIME_RESULT:
    {
      my_decimal buff;
      return to->store_decimal(val_decimal(&buff));
    }
    case STRING_RESULT:
      return save_in_field_str(to);
    case ROW_RESULT:
      break;
    }
    DBUG_ASSERT(0);
    to->reset();
    return 0;
  }
  Copy_func *get_copy_func(const Field *from) const override
  {
    // ALTER to INET6 from another field
    /*
    if (eq_def(from))
      return get_identical_copy_func();
    switch (from->cmp_type()) {
    case STRING_RESULT:
      return do_field_string;
    case TIME_RESULT:
      return do_field_temporal;
    case DECIMAL_RESULT:
      return do_field_decimal;
    case REAL_RESULT:
      return do_field_real;
    case INT_RESULT:
      return do_field_int;
    case ROW_RESULT:
      DBUG_ASSERT(0);
      break;
    }
    */
    return do_field_string;//QQ
  }

  bool memcpy_field_possible(const Field *from) const override
  {
    // INSERT INTO t1 (inet6_field) SELECT field2 FROM t2;
    return type_handler() == from->type_handler();
  }


  /*** Optimizer routines ***/
  bool test_if_equality_guarantees_uniqueness(const Item *const_item) const override
  {
    /*
      This condition:
        WHERE inet6_field=const
      should return a single distinct value only,
      as comparison is done according to INET6.
    */
    return true;
  }
  bool can_be_substituted_to_equal_item(const Context &ctx,
                                        const Item_equal *item_equal)
                                        override
  {
    switch (ctx.subst_constraint()) {
    case ANY_SUBST:
      return ctx.compare_type_handler() == item_equal->compare_type_handler();
    case IDENTITY_SUBST:
      return true;
    }
    return false;
  }
  Item *get_equal_const_item(THD *thd, const Context &ctx,
                             Item *const_item) override;
  bool can_optimize_keypart_ref(const Item_bool_func *cond,
                                const Item *item) const override
  {
    /*
      Mixing of two different non-traditional types is currently prevented.
      This may change in the future. For example, INET4 and INET6
      data types can be made comparable.
    */
    DBUG_ASSERT(item->type_handler()->is_traditional_type() ||
                item->type_handler() == type_handler());
    return true;
  }
  /**
    Test if Field can use range optimizer for a standard comparison operation:
      <=, <, =, <=>, >, >=
    Note, this method does not cover spatial operations.
  */
  bool can_optimize_range(const Item_bool_func *cond,
                          const Item *item,
                          bool is_eq_func) const override
  {
    // See the DBUG_ASSERT comment in can_optimize_keypart_ref()
    DBUG_ASSERT(item->type_handler()->is_traditional_type() ||
                item->type_handler() == type_handler());
    return true;
  }
  SEL_ARG *get_mm_leaf(RANGE_OPT_PARAM *prm, KEY_PART *key_part,
                       const Item_bool_func *cond,
                       scalar_comparison_op op, Item *value) override
  {
    DBUG_ENTER("Field_inet6::get_mm_leaf");
    if (!can_optimize_scalar_range(prm, key_part, cond, op, value))
      DBUG_RETURN(0);
    int err= value->save_in_field_no_warnings(this, 1);
    if ((op != SCALAR_CMP_EQUAL && is_real_null()) || err < 0)
      DBUG_RETURN(&null_element);
    if (err > 0)
    {
      if (op == SCALAR_CMP_EQ || op == SCALAR_CMP_EQUAL)
        DBUG_RETURN(new (prm->mem_root) SEL_ARG_IMPOSSIBLE(this));
      DBUG_RETURN(NULL); /*  Cannot infer anything */
    }
    DBUG_RETURN(stored_field_make_mm_leaf(prm, key_part, op, value));
  }
  bool can_optimize_hash_join(const Item_bool_func *cond,
                                      const Item *item) const override
  {
    return can_optimize_keypart_ref(cond, item);
  }
  bool can_optimize_group_min_max(const Item_bool_func *cond,
                                  const Item *const_item) const override
  {
    return true;
  }

  /**********/
  uint size_of() const override { return sizeof(*this); }
};


class Item_typecast_inet6: public Item_func
{
public:
  Item_typecast_inet6(THD *thd, Item *a) :Item_func(thd, a) {}

  const Type_handler *type_handler() const override
  { return &type_handler_inet6; }

  enum Functype functype() const override { return CHAR_TYPECAST_FUNC; }
  bool eq(const Item *item, bool binary_cmp) const override
  {
    if (this == item)
      return true;
    if (item->type() != FUNC_ITEM ||
        functype() != ((Item_func*)item)->functype())
      return false;
    if (type_handler() != item->type_handler())
      return false;
    Item_typecast_inet6 *cast= (Item_typecast_inet6*) item;
    return args[0]->eq(cast->args[0], binary_cmp);
  }
  const char *func_name() const override { return "cast_as_inet6"; }
  void print(String *str, enum_query_type query_type)
  {
    str->append(STRING_WITH_LEN("cast("));
    args[0]->print(str, query_type);
    str->append(STRING_WITH_LEN(" as inet6)"));
  }
  bool fix_length_and_dec()
  {
    Type_std_attributes::operator=(Type_std_attributes_inet6());
    return false;
  }
  String *val_str(String *to)
  {
    Inet6_null tmp(args[0]);
    return (null_value= tmp.is_null() || tmp.to_string(to)) ? NULL : to;
  }
  longlong val_int()
  {
    return 0;
  }
  double val_real()
  {
    return 0;
  }
  my_decimal *val_decimal(my_decimal *to)
  {
    my_decimal_set_zero(to);
    return to;
  }
  bool get_date(THD *thd, MYSQL_TIME *ltime, date_mode_t fuzzydate)
  {
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
    return false;
  }
  bool val_native(THD *thd, Native *to)
  {
    Inet6_null tmp(args[0]);
    return null_value= tmp.is_null() || tmp.to_native(to);
  }
  Item *get_copy(THD *thd)
  { return get_item_copy<Item_typecast_inet6>(thd, this); }
};


class Item_cache_inet6: public Item_cache
{
  NativeBufferInet6 m_value;
public:
  Item_cache_inet6(THD *thd)
   :Item_cache(thd, &type_handler_inet6)
  { }
  Item *get_copy(THD *thd)
  { return get_item_copy<Item_cache_inet6>(thd, this); }
  bool cache_value()
  {
    if (!example)
      return false;
    value_cached= true;
    null_value= example->val_native_with_conversion_result(current_thd,
                                                           &m_value,
                                                           type_handler());
    return true;
  }
  String* val_str(String *to)
  {
    if (!has_value())
      return NULL;
    Inet6_null tmp(m_value.ptr(), m_value.length());
    return tmp.is_null() || tmp.to_string(to) ? NULL : to;
  }
  my_decimal *val_decimal(my_decimal *to)
  {
    if (!has_value())
      return NULL;
    my_decimal_set_zero(to);
    return to;
  }
  longlong val_int()
  {
    if (!has_value())
      return 0;
    return 0;
  }
  double val_real()
  {
    if (!has_value())
      return 0;
    return 0;
  }
  longlong val_datetime_packed(THD *thd)
  {
    DBUG_ASSERT(0);
    if (!has_value())
      return 0;
    return 0;
  }
  longlong val_time_packed(THD *thd)
  {
    DBUG_ASSERT(0);
    if (!has_value())
      return 0;
    return 0;
  }
  bool get_date(THD *thd, MYSQL_TIME *ltime, date_mode_t fuzzydate)
  {
    if (!has_value())
      return true;
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
    return false;
  }
  bool val_native(THD *thd, Native *to)
  {
    if (!has_value())
      return true;
    return to->copy(m_value.ptr(), m_value.length());
  }
};


class Item_inet6_literal: public Item_literal
{
  Inet6 m_value;
public:
  Item_inet6_literal(THD *thd)
   :Item_literal(thd),
    m_value(Inet6_zero())
  { }
  Item_inet6_literal(THD *thd, const Inet6 &value)
   :Item_literal(thd),
    m_value(value)
  { }
  const Type_handler *type_handler() const override
  {
    return &type_handler_inet6;
  }
  longlong val_int() override
  {
    return 0;
  }
  double val_real() override
  {
    return 0;
  }
  String *val_str(String *to) override
  {
    return m_value.to_string(to) ? NULL : to;
  }
  my_decimal *val_decimal(my_decimal *to) override
  {
    my_decimal_set_zero(to);
    return to;
  }
  bool get_date(THD *thd, MYSQL_TIME *ltime, date_mode_t fuzzydate) override
  {
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
    return false;
  }
  bool val_native(THD *thd, Native *to) override
  {
    return m_value.to_native(to);
  }
  void print(String *str, enum_query_type query_type) override
  {
    StringBufferInet6 tmp;
    m_value.to_string(&tmp);
    str->append("INET6'");
    str->append(tmp);
    str->append('\'');
  }
  Item *get_copy(THD *thd) override
  { return get_item_copy<Item_inet6_literal>(thd, this); }

  // Non-overriding methods
  void set_value(const Inet6 &value)
  {
    m_value= value;
  }
};


class in_inet6 :public in_vector
{
  Inet6 m_value;
  static int cmp_inet6(void *cmp_arg, Inet6 *a, Inet6 *b)
  {
    return a->cmp(*b);
  }
public:
  in_inet6(THD *thd, uint elements)
   :in_vector(thd, elements, sizeof(Inet6), (qsort2_cmp) cmp_inet6, 0),
    m_value(Inet6_zero())
  { }
  const Type_handler *type_handler() const override
  {
    return &type_handler_inet6;
  }
  void set(uint pos, Item *item) override
  {
    Inet6 *buff= &((Inet6 *) base)[pos];
    Inet6_null value(item);
    if (value.is_null())
      *buff= Inet6_zero();
    else
      *buff= value;
  }
  uchar *get_value(Item *item) override
  {
    Inet6_null value(item);
    if (value.is_null())
      return 0;
    m_value= value;
    return (uchar *) &m_value;
  }
  Item* create_item(THD *thd) override
  {
    return new (thd->mem_root) Item_inet6_literal(thd);
  }
  void value_to_item(uint pos, Item *item) override
  {
    const Inet6 &buff= (((Inet6*) base)[pos]);
    static_cast<Item_inet6_literal*>(item)->set_value(buff);
  }
};


in_vector *
Type_handler_inet6::make_in_vector(THD *thd, const Item_func_in *func,
                                   uint nargs) const
{
  return new (thd->mem_root) in_inet6(thd, nargs);
}


Item *Type_handler_inet6::create_typecast_item(THD *thd, Item *item,
                                               const Type_cast_attributes &attr)
                                               const
{
  return new (thd->mem_root) Item_typecast_inet6(thd, item);
}


Item_cache *Type_handler_inet6::Item_get_cache(THD *thd, const Item *item) const
{
  return new (thd->mem_root) Item_cache_inet6(thd);
}


Item *
Type_handler_inet6::make_const_item_for_comparison(THD *thd,
                                                   Item *src,
                                                   const Item *cmp) const
{
  Inet6_null tmp(src);
  if (tmp.is_null())
    return new (thd->mem_root) Item_null(thd, src->name.str);
  return new (thd->mem_root) Item_inet6_literal(thd, tmp);
}


Item *Field_inet6::get_equal_const_item(THD *thd, const Context &ctx,
                                        Item *const_item)
{
  Inet6_null tmp(const_item);
  if (tmp.is_null())
    return NULL;
  return new (thd->mem_root) Item_inet6_literal(thd, tmp);
}


Field *
Type_handler_inet6::make_table_field_from_def(
                                     TABLE_SHARE *share,
                                     MEM_ROOT *mem_root,
                                     const LEX_CSTRING *name,
                                     const Record_addr &addr,
                                     const Bit_addr &bit,
                                     const Column_definition_attributes *attr,
                                     uint32 flags) const
{
  return new (mem_root) Field_inet6(name, addr);
}


Field *Type_handler_inet6::make_table_field(const LEX_CSTRING *name,
                                            const Record_addr &addr,
                                            const Type_all_attributes &attr,
                                            TABLE *table) const
{
  return new (table->in_use->mem_root) Field_inet6(name, addr);
}


Field *Type_handler_inet6::make_conversion_table_field(TABLE *table,
                                                       uint metadata,
                                                       const Field *target)
                                                       const
{
  const Record_addr tmp(NULL, Bit_addr(true));
  return new (table->in_use->mem_root) Field_inet6(&empty_clex_str, tmp);
}


/***************************************************************/

// QQ: This code should move to sql_type.cc

const Type_handler *
Type_handler_data::handler_by_name(const LEX_CSTRING &name) const
{
  static Name name_inet6= type_handler_inet6.name();
  if (!my_strnncoll(system_charset_info,
                    (const uchar *) name.str, name.length,
                    (const uchar *) name_inet6.ptr(), name_inet6.length()))
    return &type_handler_inet6;
  return NULL;
}


const Type_handler *
Type_handler_data::handler_by_name_or_error(const LEX_CSTRING &name) const
{
  const Type_handler *h= handler_by_name(name);
  if (!h)
    my_error(ER_UNKNOWN_DATA_TYPE, MYF(0),
             ErrConvString(name.str, name.length, system_charset_info).ptr());
  return h;
}


bool Type_handler_data::init2()
{
  return
    m_type_aggregator_for_result.add(&type_handler_inet6,
                                     &type_handler_null,
                                     &type_handler_inet6) ||
    m_type_aggregator_for_result.add(&type_handler_inet6,
                                     &type_handler_inet6,
                                     &type_handler_inet6) ||
    m_type_aggregator_for_result.add(&type_handler_inet6,
                                     &type_handler_varchar,
                                     &type_handler_inet6) ||
    m_type_aggregator_for_result.add(&type_handler_inet6,
                                     &type_handler_hex_hybrid,
                                     &type_handler_inet6) ||
    m_type_aggregator_for_comparison.add(&type_handler_inet6,
                                         &type_handler_null,
                                         &type_handler_inet6) ||
    m_type_aggregator_for_comparison.add(&type_handler_inet6,
                                         &type_handler_long_blob,
                                         &type_handler_inet6) ||
    m_type_aggregator_for_comparison.add(&type_handler_inet6,
                                         &type_handler_inet6,
                                         &type_handler_inet6);
}
