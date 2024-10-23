resource "aws_s3_bucket" "bucket" {
  bucket = "group_1"

  tags = local.tags
}
