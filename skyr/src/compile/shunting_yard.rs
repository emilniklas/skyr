use super::{BinaryOperation, BinaryOperator, Expression, HasSpan};

#[derive(Default, Debug)]
pub struct ShuntingYard {
    operators: Vec<BinaryOperator>,
    output: Vec<Member>,
}

impl ShuntingYard {
    pub fn add_expression(&mut self, expression: Expression) {
        self.output.insert(0, Member::Expression(expression));
    }

    pub fn add_operator(&mut self, o1: BinaryOperator) {
        loop {
            let o2 = match self.operators.pop() {
                None => break,

                Some(o2) => o2,
            };

            let o1p = o1.precedence();
            let o2p = o2.precedence();

            if o2p > o1p || (o1.is_left_associative() && o1p == o2p) {
                self.output.insert(0, Member::Operator(o2));
            } else {
                self.operators.push(o2);
                break;
            }
        }

        self.operators.push(o1);
    }

    pub fn resolve(mut self) -> Expression {
        while let Some(op) = self.operators.pop() {
            self.output.insert(0, Member::Operator(op));
        }

        let mut result = vec![];
        while let Some(member) = self.output.pop() {
            match member {
                Member::Expression(e) => {
                    result.push(e);
                }

                Member::Operator(operator) => {
                    let rhs = result.pop().expect("sequence error");
                    let lhs = result.pop().expect("sequence error");

                    result.push(Expression::BinaryOperation(Box::new(BinaryOperation {
                        span: lhs.span().through(&rhs.span()),
                        lhs,
                        operator,
                        rhs,
                    })))
                }
            }
        }

        assert!(result.len() == 1);
        result.remove(0)
    }
}

#[derive(Debug)]
enum Member {
    Expression(Expression),
    Operator(BinaryOperator),
}
