// Simple syntax check for CQL parser

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated, tuple},
    IResult,
};

/// CQL keyword parser - case insensitive
fn keyword(s: &str) -> impl Fn(&str) -> IResult<&str, &str> + '_ {
    move |input| tag_no_case(s)(input)
}

/// Parse whitespace
fn ws(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| c.is_whitespace())(input)
}

/// Parse identifier
fn identifier(input: &str) -> IResult<&str, String> {
    let (input, name) = alt((
        delimited(
            char('"'),
            take_while1(|c: char| c != '"'),
            char('"'),
        ),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
    ))(input)?;
    
    Ok((input, name.to_string()))
}

/// Test parser
fn parse_create_table_name(input: &str) -> IResult<&str, String> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("create")(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = keyword("table")(input)?;
    let (input, _) = ws(input)?;
    let (input, table_name) = identifier(input)?;
    
    Ok((input, table_name))
}

fn main() {
    let test_cql = "CREATE TABLE users";
    
    match parse_create_table_name(test_cql) {
        Ok((_, table_name)) => {
            println!("Successfully parsed table name: {}", table_name);
        }
        Err(e) => {
            println!("Parse error: {:?}", e);
        }
    }
    
    println!("CQL parser syntax check passed!");
}