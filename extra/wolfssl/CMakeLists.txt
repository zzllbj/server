# CMakeLists.txt
#
# Copyright (C) 2006-2015 wolfSSL Inc.
#
# This file is part of wolfSSL. (formerly known as CyaSSL)
#
# wolfSSL is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# wolfSSL is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA

SET(WOLFSSL_SRCDIR ${CMAKE_CURRENT_SOURCE_DIR}/wolfssl/src)
ADD_DEFINITIONS(${SSL_DEFINES})
SET(WOLFSSL_SOURCES  
   ${WOLFSSL_SRCDIR}/crl.c
   ${WOLFSSL_SRCDIR}/internal.c
   ${WOLFSSL_SRCDIR}/keys.c
  #${WOLFSSL_SRCDIR}/sniffer.c
   ${WOLFSSL_SRCDIR}/tls.c
   ${WOLFSSL_SRCDIR}/wolfio.c
   ${WOLFSSL_SRCDIR}/ocsp.c
   ${WOLFSSL_SRCDIR}/ssl.c)
ADD_DEFINITIONS(-DWOLFSSL_LIB -DWOLFSSL_USER_SETTINGS -DCYASSL_USER_SETTINGS -D_UNICODE -DUNICODE)
INCLUDE_DIRECTORIES(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/wolfssl)
IF(WIN32)
  INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/wolfssl/IDE/WIN)
  REMOVE_DEFINITIONS(-DHAVE_CONFIG_H)
  ADD_DEFINITIONS(-DWOLFSSL_HAVE_MIN -DWOLFSSL_HAVE_MAX)
ENDIF()
ADD_DEFINITIONS(-DWOLFSSL_SHA512 -DWOLFSSL_SHA384 -DWOLFSSL_SHA224 -DHAVE_AES_ECB -DWOLFSSL_AES_COUNTER -DWOLFSSL_AES_DIRECT)
ADD_CONVENIENCE_LIBRARY(wolfssl ${WOLFSSL_SOURCES})

IF(MSVC)
  ADD_DEFINITIONS(-wd4005) # macro redefinition WOLFSSL_HAVE_MIN
ENDIF()
SET(WOLFCRYPT_SRCDIR ${CMAKE_CURRENT_SOURCE_DIR}/wolfssl/wolfcrypt/src)
SET(WOLFCRYPT_SOURCES
${WOLFCRYPT_SRCDIR}/aes.c
${WOLFCRYPT_SRCDIR}/arc4.c
${WOLFCRYPT_SRCDIR}/asn.c
${WOLFCRYPT_SRCDIR}/blake2b.c
${WOLFCRYPT_SRCDIR}/camellia.c
${WOLFCRYPT_SRCDIR}/chacha.c
${WOLFCRYPT_SRCDIR}/coding.c
${WOLFCRYPT_SRCDIR}/compress.c
${WOLFCRYPT_SRCDIR}/des3.c
${WOLFCRYPT_SRCDIR}/dh.c
${WOLFCRYPT_SRCDIR}/dsa.c
${WOLFCRYPT_SRCDIR}/ecc.c
${WOLFCRYPT_SRCDIR}/error.c
${WOLFCRYPT_SRCDIR}/hc128.c
${WOLFCRYPT_SRCDIR}/hmac.c
${WOLFCRYPT_SRCDIR}/integer.c
${WOLFCRYPT_SRCDIR}/logging.c
${WOLFCRYPT_SRCDIR}/md2.c
${WOLFCRYPT_SRCDIR}/md4.c
${WOLFCRYPT_SRCDIR}/md5.c
${WOLFCRYPT_SRCDIR}/memory.c
${WOLFCRYPT_SRCDIR}/pkcs7.c
${WOLFCRYPT_SRCDIR}/pkcs12.c
${WOLFCRYPT_SRCDIR}/poly1305.c
${WOLFCRYPT_SRCDIR}/pwdbased.c
${WOLFCRYPT_SRCDIR}/rabbit.c
${WOLFCRYPT_SRCDIR}/random.c
${WOLFCRYPT_SRCDIR}/ripemd.c
${WOLFCRYPT_SRCDIR}/rsa.c
${WOLFCRYPT_SRCDIR}/sha.c
${WOLFCRYPT_SRCDIR}/sha256.c
${WOLFCRYPT_SRCDIR}/sha512.c
${WOLFCRYPT_SRCDIR}/tfm.c
${WOLFCRYPT_SRCDIR}/wc_port.c
${WOLFCRYPT_SRCDIR}/wc_encrypt.c
${WOLFCRYPT_SRCDIR}/hash.c
${WOLFCRYPT_SRCDIR}/wolfmath.c)
ADD_CONVENIENCE_LIBRARY(wolfcrypt ${WOLFCRYPT_SOURCES})

