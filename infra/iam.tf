data "aws_iam_policy_document" "glue_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_role_policy" {
  statement {
    actions = [
      "s3:*",
      "kms:*",
    ]
    effect = "Allow"
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_role_policy" {
  name = "group_1_glue_policy"
  path = "/"
  policy = data.aws_iam_policy_document.glue_role_policy.json
}

resource "aws_iam_role" "glue" {
  name                = "group_1_glue_role"
  assume_role_policy  = data.aws_iam_policy_document.glue_assume_role_policy.json
  managed_policy_arns = [aws_iam_policy.glue_role_policy.arn]
}