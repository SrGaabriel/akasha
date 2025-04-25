pub mod lexer;
pub mod parser;
pub mod err;

fn and_then<'a, A, B, F, G, H: 'a>(
    first: F,
    second: G,
) -> impl Fn(&'a H) -> Option<((A, B), &'a H)>
where
    F: Fn(&'a H) -> Option<(A, &'a H)> + 'a,
    G: Fn(&'a H) -> Option<(B, &'a H)> + 'a,
{
    move |input| {
        first(input).and_then(|(a, rest1)| {
            second(rest1).map(|(b, rest2)| ((a, b), rest2))
        })
    }
}
