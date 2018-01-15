package main

import (
	"github.com/nuclio/nuclio-sdk"
	"net/http"
	"net/url"
	"crypto/rsa"
	"github.com/dgrijalva/jwt-go"
	"time"
	"strings"
	"html/template"
	"fmt"
	"bytes"
)

var (
	verifyKey *rsa.PublicKey
	signKey   *rsa.PrivateKey
)

type NuclioClaims struct {
	*jwt.StandardClaims
	Roles []string
}

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	path := event.GetPath()
	headers := map[string]interface{}{}

	context.Logger.InfoWith("Log", "path", event.GetPath(), "content", event.GetHeaders())
	context.Logger.InfoWith("Log", "body", string(event.GetBody()), "x-uri", event.GetHeaderString("X-Original-Uri"))

	// Check authentication token and path permissions
	authToken := event.GetHeaderString("Authorization")
	if  authToken != "" {
		// initialize public key if not initialized
		if verifyKey == nil {
			var err error
			verifyKey, err = jwt.ParseRSAPublicKeyFromPEM([]byte(PublicKey))
			if err != nil {
				context.Logger.Error("Failed to prep PrivateKey")
				return nil, err
			}
		}

		// check if the token is valid (signature, expiration, ..)
		token, err := verifyToken(authToken)
		if err != nil {
			context.Logger.ErrorWith("Unauthorized Token", "token", authToken, "err", err)
			return nuclio.Response{	StatusCode:  http.StatusUnauthorized,
				Body: []byte("failed verify token") }, nil
		}

		// return the authenticated user and its roles, path access credential to be used cached by Nginx
		// (this function will be called by nginx for every new path and cache auth timeout)
		headers["user"] = token.Claims.(*NuclioClaims).Subject
		headers["roles"] = strings.Join(token.Claims.(*NuclioClaims).Roles, ",")
		headers["access"] = "ro"

		// test access permission, need to take into consideration identity (user/roles, path, oper)
		// this is just an example
		if event.GetHeaderString("X-Original-Uri")=="/b" {
			context.Logger.Error("access to /b is Forbidden")
			return nuclio.Response{	StatusCode:  http.StatusForbidden, Headers:headers }, nil
		}

		return nuclio.Response{	StatusCode:  200,
			Headers: headers }, nil
	}

	// verify login form user and password, return JWT token
	if len(path) > 5 && path[0:6] == "/login" {
		values, err := url.ParseQuery(string(event.GetBody()))
		if err != nil {
			return nil, err
		}
		user := values.Get("user")
		passwd := values.Get("passwd")

		if user != "q" || passwd != "a" {
			// TODO: try login again
			context.Logger.ErrorWith("Unauthorized", "user", user, "passwd", passwd)
			return nuclio.Response{	StatusCode:  http.StatusUnauthorized,
				Body: getTemplate("Login failed! Try Again"), ContentType: "text/html" }, nil
		}

		if signKey == nil {
			signKey, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(PrivateKey))
			if err != nil {
				context.Logger.Error("Failed to prep PrivateKey")
				return nil, err
			}
		}

		// generate new signed token and add user roles to it
		token, err := getToken(user, []string{"admin","stam"})
		if err != nil {
			context.Logger.ErrorWith("Invalid token", "err", err)
			return nil, err
		}

		context.Logger.InfoWith("generated token", "token", token)

		headers["token"] = token

		return nuclio.Response{	StatusCode:  200,
			Headers: headers }, nil
	}

	// return Login Form
	return nuclio.Response{	StatusCode:  200,
		Body: getTemplate(""),
		//Headers: hdr,
		ContentType: "text/html"}, nil
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

func getTemplate(text string) []byte {
	t := template.New("login")
	t, _ = t.Parse(loginForm)
	if text != "" {
		text = fmt.Sprintf(`<font color="red">%s</font><br><br>`, text)
	}
	var buf bytes.Buffer
	t.Execute(&buf, text)
	return buf.Bytes()
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

const loginForm  = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Login Form</title>
</head>
<style>
form {
    border: 3px solid #f1f1f1;
    max-width: 400px;
    margin: auto;
}

input[type=text], input[type=password] {
    width: 100%;
    padding: 12px 20px;
    margin: 8px 0;
    display: inline-block;
    border: 1px solid #ccc;
    box-sizing: border-box;
}

button {
    background-color: #007adf;
    color: white;
    padding: 14px 20px;
    margin: 8px 0;
    border: none;
    cursor: pointer;
    width: 100%;
}

button:hover {
    opacity: 0.8;
}

.container {
    padding: 16px;
}

</style>
<body>


<form method="post" action="/login">

  <div class="container">
    <h1>Login Form</h1>{{.}}
    <label><b>Username</b></label>
    <input type="text" placeholder="Enter Username" name="user" required>

    <label><b>Password</b></label>
    <input type="password" placeholder="Enter Password" name="passwd" required>

    <button type="submit">Login</button>
    <label>
      <input type="checkbox" checked="checked"> Remember me
    </label>
  </div>

</form>

</body>
</html>
`