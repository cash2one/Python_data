# -*- coding:utf-8 -*-

import sys
import crypto
import base64

#字符串(账户)加密解密
class StringEncrypt():
    def __init__(self):
        pass

    #生成公钥和私钥
    def initkey(self):
        key = crypto.newkeys(128, accurate=True, poolsize=1)
        crypto.export_key_file(key[0], 'PublicKey.rsa')
        crypto.export_key_file(key[1], 'PrivateKey.rsa')

    #使用公钥加密字符串
    def encrypt(self, destring, pubKeyFile):
        pub_key = crypto.load_key_file(pubKeyFile)
        enString = crypto.encrypt_str(destring, pub_key, encode=base64.standard_b64encode)
        return enString

    #使用私钥解密字符串
    def decrypt(self, enString, priKeyFile):
        pri_key = crypto.load_key_file(priKeyFile)
        deString = crypto.decrypt_str(enString, pri_key, decode=base64.standard_b64decode)
        return deString

if __name__ == '__main__':
    if(not sys.argv[1]):
        print 'use en to encrypt,use de to decrypt!'
        exit()
    if(not sys.argv[2]):
        print 'argv2 is string to encrypt or decrypt!'
        exit()
    action = sys.argv[1]
    string = sys.argv[2]
    encrypt = StringEncrypt()
    if(action == 'en'):
        enString = encrypt.encrypt(string, 'PublicKey.rsa')
        print enString
    elif(action == 'de'):
        deString = encrypt.decrypt(string, 'PrivateKey.rsa')
        print deString
    elif(action == 'init'):
        encrypt.initkey()
        print 'Create new RSA public,private key success!'
    else:
        print 'use en to encrypt,use de to decrypt!'
	
