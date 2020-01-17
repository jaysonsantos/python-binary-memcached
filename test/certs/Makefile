# chain     :: pem <= []crt
# dhparam   :: pem <= int (#bits)
# gen-ca    :: crt <= conf, key
# gen-csr   :: csr <= conf, key
# gen-key    :: key <= int (#bits)
# gen-ecckey :: key <= string (ECC Prime)
# self-sign :: crt <= days, hash, extensions, conf, csr, key (self)
# sign      :: crt <= days, hash, extensions, conf, csr, key (CA), crt (CA)

# Note: Files and paths must not contain spaces.

DOMAIN = localhost
DEFAULT_CERT_TYPE = rsa2048
DEFAULT_DAYS = 730
DEFAULT_HASH = sha256

DEFAULT_CA_SIGN = 3650 $(DEFAULT_HASH) req_v3_ca
DEFAULT_server_SIGN = $(DEFAULT_DAYS) $(DEFAULT_HASH) req_v3_usr

.PHONY: all
all: chains

.PHONY: clean
clean:
	rm -rf gen

# root CA #############################
gen/key/ca-root.key:
	./tool gen-key $@ $(DOMAIN) 4096
gen/crt/ca-root.crt: conf/ca-root.conf gen/key/ca-root.key
	./tool gen-ca $@ $(DOMAIN) $^

# intermediate CA #####################
gen/key/ca-intermediate.key:
	./tool gen-key $@ $(DOMAIN) 4096
gen/csr/ca-intermediate.csr: conf/ca-intermediate.conf gen/key/ca-intermediate.key
	./tool gen-csr $@ $(DOMAIN) $^
gen/crt/ca-intermediate.crt: conf/ca-intermediate.conf gen/csr/ca-intermediate.csr gen/key/ca-root.key gen/crt/ca-root.crt
	./tool sign $@ $(DOMAIN) $(DEFAULT_CA_SIGN) $^

# server cert #########################
gen/key/server-rsa2048.key:
	./tool gen-key $@ $(DOMAIN) 2048
gen/csr/server-rsa2048.csr: conf/server.conf gen/key/server-rsa2048.key
	./tool gen-csr $@ $(DOMAIN) $^
gen/crt/server-rsa2048.crt: conf/server.conf gen/csr/server-rsa2048.csr gen/key/ca-intermediate.key gen/crt/ca-intermediate.crt
	./tool sign $@ $(DOMAIN) $(DEFAULT_server_SIGN) $^
gen/chain/server-rsa2048.pem: gen/crt/server-rsa2048.crt gen/crt/ca-intermediate.crt
	./tool chain $@ $(DOMAIN) $^
CHAINS += gen/chain/server-rsa2048.pem

# client root CA ######################
gen/key/client-ca-root.key:
	./tool gen-key $@ $(DOMAIN) 4096
gen/crt/client-ca-root.crt: conf/client-ca-root.conf gen/key/client-ca-root.key
	./tool gen-ca $@ $(DOMAIN) $^

# client cert #########################
gen/key/client.key:
	./tool gen-key $@ $(DOMAIN) 2048
gen/csr/client.csr: conf/client.conf gen/key/client.key
	./tool gen-csr $@ $(DOMAIN) $^
gen/crt/client.crt: conf/client.conf gen/csr/client.csr gen/key/client-ca-root.key gen/crt/client-ca-root.crt
	./tool sign $@ $(DOMAIN) $(DEFAULT_server_SIGN) $^
gen/crt/client.p12: gen/crt/client.crt gen/key/client.key
	./tool gen-pkcs12-p12 $@ $(DOMAIN) $^
gen/chain/client.pem: gen/crt/client.p12
	./tool pkcs12-convert-p12-pem $@ $(DOMAIN) $^
CHAINS += gen/chain/client.pem

#######################################

.PHONY: chains
chains: $(CHAINS)
