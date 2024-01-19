// Simple tokenizer implementation that will remove any non-alphabetic and non-alphanumeric characters from the text, and
// then split the text by whitespace

#[derive(Debug)]
pub struct Token<'a> {
    text: &'a str,
}

impl<'a> Token<'a> {
    pub fn new(text: &'a str) -> Token<'a> {
        Token { text }
    }

    pub fn clean(&self) -> String {
        return self.text
            .to_lowercase()
            .chars()
            .filter(|c| c.is_alphabetic() || c.is_alphanumeric())
            .collect()
        ;
    }
}

pub fn tokenize(input_str: &str) -> Vec<Token> {
    input_str
        .split_whitespace()
        .map(Token::new)
        .collect()
}