provider "aws" {
  region = var.aws_region
}

# Obter informações sobre a conta AWS (ID da conta, ARN, etc.)
data "aws_caller_identity" "current" {}

# Recuperar valores do SSM
data "aws_ssm_parameter" "cognito_user_pool_id" {
  name = "/video-frame-pro/cognito/user_pool_id"
}

data "aws_ssm_parameter" "cognito_client_id" {
  name = "/video-frame-pro/cognito/client_id"
}

# Função Lambda para Registro de Usuário
resource "aws_lambda_function" "register_user" {
  function_name = var.lambda_register_name

  handler = "register.lambda_handler"
  runtime = "python3.8"
  role    = aws_iam_role.lambda_register_role.arn

  environment {
    variables = {
      cognito_user_pool_id = data.aws_ssm_parameter.cognito_user_pool_id.value
      cognito_client_id    = data.aws_ssm_parameter.cognito_client_id.value
    }
  }

  filename         = "../lambda/register/register_lambda_function.zip"
  source_code_hash = filebase64sha256("../lambda/register/register_lambda_function.zip")
}

# Função Lambda para Login de Usuário
resource "aws_lambda_function" "login_user" {
  function_name = var.lambda_login_name

  handler = "login.lambda_handler"
  runtime = "python3.8"
  role    = aws_iam_role.lambda_login_role.arn

  environment {
    variables = {
      cognito_user_pool_id = data.aws_ssm_parameter.cognito_user_pool_id.value
      cognito_client_id    = data.aws_ssm_parameter.cognito_client_id.value
    }
  }

  filename         = "../lambda/login/login_lambda_function.zip"
  source_code_hash = filebase64sha256("../lambda/login/login_lambda_function.zip")
}

# Grupos de logs para as funções Lambda
resource "aws_cloudwatch_log_group" "lambda_register_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.register_user.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "lambda_login_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.login_user.function_name}"
  retention_in_days = var.log_retention_days
}

# Role para Lambda de Registro
resource "aws_iam_role" "lambda_register_role" {
  name = "lambda_register_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Role para Lambda de Login
resource "aws_iam_role" "lambda_login_role" {
  name = "lambda_login_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Política de Permissões do Cognito para Lambda
resource "aws_iam_policy" "lambda_cognito_policy" {
  name        = "lambda_cognito_policy"
  description = "Permissões necessárias para as Lambdas interagirem com o Cognito"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "cognito-idp:SignUp",
          "cognito-idp:InitiateAuth",
          "cognito-idp:AdminCreateUser",
          "cognito-idp:RespondToAuthChallenge",
          "cognito-idp:AdminUpdateUserAttributes",
          "lambda:GetFunction"
        ],
        Effect   = "Allow",
        Resource = var.cognito_user_pool_arn
      },
      {
        Action = [
          "cognito-idp:AdminConfirmSignUp",
          "cognito-idp:AdminSetUserPassword",
          "cognito-idp:AdminGetUser"
        ],
        Effect   = "Allow",
        Resource = var.cognito_user_pool_arn
      }
    ]
  })
}

# Política de Permissões para CloudWatch Logs
resource "aws_iam_policy" "lambda_logging_policy" {
  name        = "lambda_logging_policy"
  description = "Permissões para Lambdas gravarem nos logs do CloudWatch"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"
      }
    ]
  })
}

# Anexar a política de logs à role da Lambda de Registro
resource "aws_iam_role_policy_attachment" "register_logging_policy_attachment" {
  role       = aws_iam_role.lambda_register_role.name
  policy_arn = aws_iam_policy.lambda_logging_policy.arn
}

# Anexar a política de logs à role da Lambda de Login
resource "aws_iam_role_policy_attachment" "login_logging_policy_attachment" {
  role       = aws_iam_role.lambda_login_role.name
  policy_arn = aws_iam_policy.lambda_logging_policy.arn
}

# Anexar a política do Cognito à role da Lambda de Registro
resource "aws_iam_role_policy_attachment" "register_policy_attachment" {
  role       = aws_iam_role.lambda_register_role.name
  policy_arn = aws_iam_policy.lambda_cognito_policy.arn
}

# Anexar a política do Cognito à role da Lambda de Login
resource "aws_iam_role_policy_attachment" "login_policy_attachment" {
  role       = aws_iam_role.lambda_login_role.name
  policy_arn = aws_iam_policy.lambda_cognito_policy.arn
}