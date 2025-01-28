output "lambda_register_function_name" {
  value = aws_lambda_function.register_user.function_name
}

output "lambda_register_function_arn" {
  value = aws_lambda_function.register_user.arn
}

output "lambda_login_function_name" {
  value = aws_lambda_function.login_user.function_name
}

output "lambda_login_function_arn" {
  value = aws_lambda_function.login_user.arn
}