# Região da AWS
variable "aws_region" {
  description = "Região onde os recursos serão provisionados"
  type        = string
  default     = "us-east-1"
}

# Nome da Lambda de Registro
variable "lambda_register_name" {
  description = "Nome da função Lambda de registro"
  type        = string
}

# Nome da Lambda de Login
variable "lambda_login_name" {
  description = "Nome da função Lambda de login"
  type        = string
}

variable "cognito_user_pool_arn" {
  description = "ARN do Cognito User Pool"
  type        = string
}

# Tempo de retenção dos logs
variable "log_retention_days" {
  description = "Número de dias para reter logs no CloudWatch"
  default     = 7
}