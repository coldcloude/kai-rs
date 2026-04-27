use std::sync::OnceLock;

use crate::{error::Result, tokenizer::Tokenizer};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum SubstringType {
    LETTER,
    DIGIT,
    OTHER,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct SubstringToken {
    pub token: String,
    pub token_type: SubstringType,
}

pub struct SubstringTokenizer {

}

struct TokensParser {
    tokens: Vec<SubstringToken>,
    current_token: String,
    last_token_type: SubstringType,
    last_is_upper_case: bool,
}

impl TokensParser {
    pub fn new() -> Self {
        Self {
            tokens: Vec::new(),
            current_token: String::new(),
            last_token_type: SubstringType::OTHER,
            last_is_upper_case: false,
        }
    }

    fn end(&mut self) {
        if !self.current_token.is_empty() {
            self.tokens.push(SubstringToken {
                token: self.current_token.clone(),
                token_type: self.last_token_type.clone(),
            });
        }
        self.current_token = String::new();
        self.last_token_type = SubstringType::OTHER;
        self.last_is_upper_case = false;
    }

    fn add(&mut self, c: char, token_type: SubstringType, is_upper_case: bool) {
        self.current_token.push(c);
        self.last_token_type = token_type;
        self.last_is_upper_case = is_upper_case;
    }

    pub fn parse(&mut self, content: &str) {
        for c in content.chars() {
            //首先判断每个字符的类型
            let token_type =
            if c.is_ascii_alphabetic() {
                SubstringType::LETTER
            }
            else if c.is_ascii_digit() {
                SubstringType::DIGIT
            }
            else {
                SubstringType::OTHER
            };
            let is_upper_case = c.is_ascii_uppercase();

            //小写可以接任意字母后，大写只能接大写后，数字只能接数字后，其他都不能接
            match token_type {
                SubstringType::LETTER => {
                    if self.last_token_type != SubstringType::LETTER || is_upper_case && !self.last_is_upper_case {
                        self.end();
                    }
                }
                SubstringType::DIGIT => {
                    if self.last_token_type != SubstringType::DIGIT {
                        self.end();
                    }
                }
                SubstringType::OTHER => {
                    self.end();
                }
            }

            //保存当前字符信息
            self.add(if is_upper_case { c.to_ascii_lowercase() } else { c }, token_type, is_upper_case);
        }

        //保存最后一个token
        self.end();
    }
}

static SUBSTRING_TOKENIZER: OnceLock<SubstringTokenizer> = OnceLock::new();

impl SubstringTokenizer {
    pub fn get() -> &'static Self {
        SUBSTRING_TOKENIZER.get_or_init(|| SubstringTokenizer {})
    }
}

impl Tokenizer<SubstringToken> for SubstringTokenizer {
    fn tokenize(&self, content: &str) -> Vec<SubstringToken> {
        let mut tokens_parser = TokensParser::new();
        let parts = content.split_ascii_whitespace();
        for part in parts {
            tokens_parser.parse(part);
        }
        tokens_parser.tokens
    }

    fn untokenize(&self, tokens: &[SubstringToken]) -> Result<String> {
        let mut result = String::new();
        let mut last_token_type = SubstringType::OTHER;
        for token in tokens {
            //字母之间或数字之间要加空格，否则会变成一个token
            match token.token_type {
                SubstringType::LETTER => {
                    if last_token_type == SubstringType::LETTER {
                        result.push(' ');
                    }
                }
                SubstringType::DIGIT => {
                    if last_token_type == SubstringType::DIGIT {
                        result.push(' ');
                    }
                }
                SubstringType::OTHER => {}
            }
            result.push_str(&token.token);
            last_token_type = token.token_type.clone();
        }
        Ok(result)
    }
}
