use std::str::FromStr;

use secp256k1::{
    ecdsa::Signature,
    hashes::{sha256, Hash},
    All, Error, Message, Secp256k1, SecretKey,
};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct Signer {
    context: Secp256k1<All>,
    secret_key: SecretKey,
}

impl FromStr for Signer {
    type Err = Error;
    fn from_str(s: &str) -> Result<Signer, Error> {
        let secret_key = SecretKey::from_str(s)?;
        let context = Secp256k1::new();
        Ok(Signer {
            context,
            secret_key,
        })
    }
}

impl Signer {
    pub fn sign<T: Serialize>(&self, message: &T) -> anyhow::Result<String> {
        let message = bincode::serialize(message)?;
        let msg = sha256::Hash::hash(&message);
        let msg = Message::from_digest_slice(msg.as_ref())?;
        let sig = self.context.sign_ecdsa(&msg, &self.secret_key);
        Ok(sig.to_string())
    }

    pub fn verify<T: Serialize>(&self, message: &T, signature: String) -> anyhow::Result<bool> {
        let message = bincode::serialize(message)?;
        let msg = sha256::Hash::hash(&message);
        let msg = Message::from_digest_slice(msg.as_ref())?;
        let signature = Signature::from_str(&signature)?;
        let public_key = self.secret_key.public_key(&self.context);
        Ok(self
            .context
            .verify_ecdsa(&msg, &signature, &public_key)
            .is_ok())
    }
}
