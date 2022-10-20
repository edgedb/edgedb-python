select schema::Function {
  Name := .name,
  Language := .language,
  Params := .params {
    Name := .name,
    Default := .default,
  }
}
limit 1;
