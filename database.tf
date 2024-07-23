resource "aws_db_instance" "default" {
  allocated_storage    = 10
  identifier           = "dec-hackathon"
  db_name              = "dec_hackathon_db"
  engine               = "postgres"
  engine_version       = "16.1"
  instance_class       = "db.t3.micro"
  username             = "bimmy"
  password             = aws_ssm_parameter.dec_hackthon_pwd_source.value
  skip_final_snapshot  = true
   }


resource "random_password" "dec_hackathon" {
  length           = 20
  special          = false
  }

resource "aws_ssm_parameter" "dec_hackthon_pwd_source" {
  name  = "DEC_hackathon_db_pwd"
  type  = "String"
  value = random_password.dec_hackathon.result
} 