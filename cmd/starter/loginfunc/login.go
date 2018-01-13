package main

//  Generate RSA signing files via shell (adjust as needed):
//
//  $ openssl genrsa -out app.rsa 1024
//  $ openssl rsa -in app.rsa -pubout > app.rsa.pub
//
// Code borrowed and modified from the following sources:
// https://www.youtube.com/watch?v=dgJFeqeXVKw
// https://goo.gl/ofVjK4
// https://github.com/dgrijalva/jwt-go
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"crypto/rsa"
	"github.com/dgrijalva/jwt-go"
	"encoding/json"
	"strings"
)

const (
	privKeyPath = "app.rsa"
	pubKeyPath  = "app.rsa.pub"
)

var (
	verifyKey *rsa.PublicKey
	signKey   *rsa.PrivateKey
)

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func initKeys() {
	signBytes, err := ioutil.ReadFile(privKeyPath)
	fatal(err)

	signKey, err = jwt.ParseRSAPrivateKeyFromPEM(signBytes)
	fatal(err)

	verifyBytes, err := ioutil.ReadFile(pubKeyPath)
	fatal(err)

	verifyKey, err = jwt.ParseRSAPublicKeyFromPEM(verifyBytes)
	fatal(err)
}

type NuclioClaims struct {
	*jwt.StandardClaims
	Roles []string
}

func main() {

	initKeys()
	t, err := getToken("joe", []string{"admin","stam"})
	if err != nil {
		fmt.Println("Invalid token:", err)
		return
	}
	fmt.Println("Token:", t)

	vtoken, err := verifyToken(t)
	if err != nil {
		fmt.Println("not verified:", err)
		return
	}
	fmt.Println("Token Verified :)")

	claims, _ := json.Marshal(vtoken.Claims)
	fmt.Println("Claims:", string(claims))
	fmt.Println("Subject:", vtoken.Claims.(*NuclioClaims).Subject)
	fmt.Println("Roles:", vtoken.Claims.(*NuclioClaims).Roles)

}

func getToken(user string, roles []string) (string, error) {
	token := jwt.New(jwt.SigningMethodRS256)

	token.Claims = &NuclioClaims{ StandardClaims: &jwt.StandardClaims{
		// set the expire time to +5 min
		ExpiresAt: time.Now().Add(time.Minute * 5).Unix(),
		Subject: user,
		Issuer: "nuclio",
		},
		Roles: roles,
}

	return token.SignedString(signKey)
}


func verifyToken(tokenString string) (*jwt.Token, error) {
	if len(tokenString) > 6 && strings.ToUpper(tokenString[0:7]) == "BEARER " {
		tokenString =  tokenString[7:]
	}
	return jwt.ParseWithClaims(tokenString, &NuclioClaims{}, func(token *jwt.Token) (interface{}, error) {
		// TBD extra verifications
		return verifyKey, nil
	})

}

func extractToken(tok string) (string, error) {
	if len(tok) > 6 && strings.ToUpper(tok[0:7]) == "BEARER " {
		return tok[7:], nil
	}
	return tok, nil

}

const PublicKey = `
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQChklQW6T3jpTeY/DdpnUVp4ltD
daCDaOb0uHT8ap9trL8qrQu99nxJ0/lsR2Xe4NK0cn91Hi2knHht5l8WsjwzFr18
Qwo6OQsnP9gSHBx9OOiBwdvjMTW/NiuGjAxq8vhz1+Wi0jS9B5TKAjAgDvMUr5Yu
lWben8Flj/Wc8xIPcQIDAQAB
-----END PUBLIC KEY-----`

const PrivateKey = `
-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQChklQW6T3jpTeY/DdpnUVp4ltDdaCDaOb0uHT8ap9trL8qrQu9
9nxJ0/lsR2Xe4NK0cn91Hi2knHht5l8WsjwzFr18Qwo6OQsnP9gSHBx9OOiBwdvj
MTW/NiuGjAxq8vhz1+Wi0jS9B5TKAjAgDvMUr5YulWben8Flj/Wc8xIPcQIDAQAB
AoGAVdI+/kh4CkJJDObznBLguwR0C5ogX4zKGLUd1rHf60a16DowoX+atzB0LTKj
hsaOxTCISTJ1dhly0pVqbsjPtk86ZqsVHS6ZpEtukBJyP2Rxso34FNK8ELHxBqWF
8bZRApTc10lsabw1nEVbMv4wCFTH2JL1H0pWZfoKPW+uBFECQQDRK/Pz4rYCx0T6
QY0SQeLmi3YysNN9wSZZFSV2Rtb2SQJLosuXYeYf2Jqwjft9tIbxnM2MDgCzHzcR
mf1HbHE9AkEAxb5Rrh49udvgh0R/MSa9bLRuxkRcUPi3Q9bq0kW/yRldKd5tnBb6
/np88Mj9zxdeJfzenWSHTFkcKCca9fpSRQJBALDF7oOcg6nRcm/02h4l5HZmQlwQ
qIvMonYCswhEUgMOLIxzZo/nQq6DRigTtgjEeo7Sr7u/UcQPi2CqvSyRbxkCQHZQ
RTbUyUHnGRRWG3QhJb1gx9bBGCsqZRyl/qIJMZL1JhTjDNoTteGFEDRRTesWpDI+
dkPd3kYEGyC7JRp/x7ECQQCHq+a+iGDGT3UaJ7mUFIqeMCPkpDuWAQE1stCoTR6v
f6G+Tj6bcveQU/rWUxdJcEEljunlhQgHw0rKJQtwwEsH
-----END RSA PRIVATE KEY-----
`