output "database_endpoint" {
  description = "The endpoint of the database"
  value = aws_db_instance.c9_velo_deloton_db.address
}