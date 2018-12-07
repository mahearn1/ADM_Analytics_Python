import argparse
import os
import hashlib
import rsa
import sys
from base64 import b64encode, b64decode

##################
#
# CREATED DATE:  03/23/2018
# DESCRIPTION :  Handles password encryption:
#
# Usage:
# To create new encryption key:
# python  passwordEncrypt.py -type RESET
#
# Encrypt a password:
# python  passwordEncrypt.py -type SET -password ccc
# Encrypted: "Encrypted: "vZCAiGHh6Uc6neHDvUXoISGEoeWXWlxUYLck14Uz3apMXpzfjNu2YGxE8OYflIi65BhDZrAxfqhzXGyCNmYGTA=="
#
# Decrypt password:
# python passwordEncrypt.py -type TEST -password "Encrypted: "vZCAiGHh6Uc6neHDvUXoISGEoeWXWlxUYLck14Uz3apMXpzfjNu2YGxE8OYflIi65BhDZrAxfqhzXGyCNmYGTA=="
# Decrypted: ccc
# 
#
# The quotes around parameters are optional
##################


def resetPublicPrivateKey():
    (key_pub, key_priv) = rsa.newkeys(512)
    privateKey = open("privateKey.pem", 'w')
    privateKey.write(key_priv.save_pkcs1())
    privateKey.close()


def setPassword( pPassword):
    with open('privateKey.pem', mode='rb') as privatefile:
        keydata = privatefile.read()
        key_priv = rsa.PrivateKey.load_pkcs1(keydata)

    encrypted = b64encode(rsa.encrypt(pPassword, key_priv))
    print 'Encrypted: "' + encrypted + '"'

def testPassword( pEncryptedPassword):
    with open('privateKey.pem', mode='rb') as privatefile:
        keydata = privatefile.read()
        key_priv = rsa.PrivateKey.load_pkcs1(keydata)
    try:
        decrypted = rsa.decrypt(b64decode(pEncryptedPassword), key_priv)
        print 'Decrypted: '+ decrypted
    except Exception, err:
        print 'Decryption Error: ' + str(err)

    
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description = '')
    parser.add_argument('-type',required=True,choices=['SET', 'RESET','TEST'])
    parser.add_argument('-password')
    args = parser.parse_args()

    if args.type.upper() == 'RESET' :
        print 'Key is in the file privateKey.pem'
        resetPublicPrivateKey()
    if args.type.upper() == 'SET':
        setPassword( args.password)
    if args.type.upper() == 'TEST':
        testPassword( args.password)
