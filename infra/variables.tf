variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "Região da AWS onde os recursos serão implantados"
}

variable "project_name" {
  type        = string
  default     = "portfolio-ecommerce"
  description = "Nome do projeto para composição de tags e recursos"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Ambiente de implantação (ex: dev, prod)"
}

variable "glue_version" {
    type = string
    default = "4.0" # Suporta Spark 3.3.0 e Python 3.10
    description = "Versão do glue"
}

variable "glue_worker_type" {
    type = string
    default = "G.1X" # Mínimo para economizar custos durante testes
    description = "Tipo de worker"
}

variable "glue_number_of_workers" {
    type = string
    default = "2"
    description = "Quantidade de workers por tipo"
}