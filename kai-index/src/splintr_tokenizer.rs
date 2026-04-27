use crate::{error::Result, tokenizer::Tokenizer};

pub struct SplintrTokenizer {
    tokenizer: splintr::Tokenizer,
}

impl SplintrTokenizer {
    pub fn new(tokenizer_name: &str) -> Result<Self> {
        let encoder = splintr::pretrained::from_pretrained(tokenizer_name)?;
        Ok(Self {
            tokenizer: encoder,
        })
    }
}

impl Tokenizer<u32> for SplintrTokenizer {
    fn tokenize(&self, content: &str) -> Vec<u32> {
        self.tokenizer.encode(content)
    }

    fn untokenize(&self, tokens: &[u32]) -> Result<String> {
        let r = self.tokenizer.decode(tokens)?;
        Ok(r)
    }
}
