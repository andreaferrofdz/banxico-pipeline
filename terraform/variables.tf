variable "project_name" {
  description = "Nombre del proyecto, usado como prefijo en todos los recursos"
  type        = string
  default     = "banxico-pipeline"
}

variable "environment" {
  description = "Ambiente de despliegue"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Región AWS donde se despliegan los recursos"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile a usar para autenticación"
  type        = string
  default     = "andre-admin"
}

variable "glue_dpu" {
  description = "Número de DPUs para jobs de Glue. Mínimo 2 para reducir costo."
  type        = number
  default     = 2
}

variable "banxico_series" {
  description = "Series del Banxico SIE a procesar"
  type        = map(string)
  default = {
    tipo_de_cambio = "SF43718"
    tiie_28        = "SF61745"
    inpc           = "SP1"
  }
}

variable "alert_email" {
  description = "Email para alertas de SNS cuando falla calidad de datos"
  type        = string
}