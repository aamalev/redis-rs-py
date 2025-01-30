#[derive(Default, PartialEq, Eq, Debug)]
pub struct Params {
    pub keys: Vec<Vec<u8>>,
    pub block: bool,
}

impl From<&[u8]> for Params {
    fn from(value: &[u8]) -> Self {
        Self {
            keys: vec![value.to_vec()],
            ..Default::default()
        }
    }
}

impl From<&redis::Cmd> for Params {
    fn from(cmd: &redis::Cmd) -> Self {
        let mut result = Self::default();
        let mut cmd_iter = cmd.args_iter().filter_map(|arg| match arg {
            redis::Arg::Simple(s) => Some(s),
            _ => None,
        });
        let key = match cmd_iter.next() {
            Some(b"CLUSTER") => match cmd_iter.next() {
                Some(b"KEYSLOT") => cmd_iter.next(),
                _ => None,
            },
            Some(b"XGROUP" | b"XINFO") => {
                cmd_iter.next();
                cmd_iter.next()
            }
            Some(b"ZMPOP") => {
                result.block = true;
                cmd_iter.next();
                cmd_iter.next()
            }
            Some(b"ZDIFF" | b"ZINTER" | b"ZINTERCARD" | b"ZINTERSTORE" | b"ZUNION") => {
                cmd_iter.next();
                cmd_iter.next()
            }
            Some(b"BZMPOP") => {
                result.block = true;
                cmd_iter.next();
                cmd_iter.next();
                cmd_iter.next()
            }
            Some(b"EVAL" | b"ZDIFFSTORE" | b"ZUNIONSTORE") => {
                cmd_iter.next();
                cmd_iter.next();
                cmd_iter.next()
            }
            Some(b"XREAD" | b"XREADGROUP") => {
                loop {
                    match cmd_iter.next() {
                        Some(b"BLOCK") => result.block = true,
                        Some(b"STREAMS") | None => break,
                        _ => continue,
                    }
                }
                cmd_iter.next()
            }
            Some(b"MIGRATE") => {
                loop {
                    if let Some(b"KEYS") | None = cmd_iter.next() {
                        break;
                    }
                }
                cmd_iter.next()
            }
            Some(b"INFO" | b"CLIENT" | b"KEYS") => None,
            Some(b"BLPOP" | b"BRPOP") => {
                result.block = true;
                cmd_iter.next()
            }
            _ => cmd_iter.next(),
        };
        if let Some(key) = key {
            result.keys.push(key.to_vec());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::Params;

    #[test]
    fn cmd_to_param_get() {
        let cmd = redis::cmd("GET").arg("a").to_owned();
        let param = Params::from(&cmd);

        let result: Params = b"a".as_ref().into();

        assert_eq!(param, result);
    }

    #[test]
    fn cmd_to_param_blpop() {
        let cmd = redis::cmd("BLPOP").arg("a").to_owned();
        let param = Params::from(&cmd);

        let mut result: Params = b"a".as_ref().into();
        result.block = true;

        assert_eq!(param, result);
    }
}
