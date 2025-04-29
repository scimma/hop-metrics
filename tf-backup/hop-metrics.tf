locals {
  hop-metrics-tags = {
    Service     = "hop-metrics"
    Criticality = "Production"
    OwnerEmail  = "fabs@illinois.edu"

    createdBy = var.createdBy
    repo      = var.repo
    lifetime  = var.lifetime
  }
}

resource "aws_ecr_repository" "hop-metrics" {
  name = "scimma/hop-metrics"
  tags = local.hop-metrics-tags
}

resource "kubernetes_cron_job_v1" "hop-metrics" {
  metadata {
    name = lower("${var.namePrefix}-hop-metrics")
  }
  spec {
    concurrency_policy            = "Forbid"
    failed_jobs_history_limit     = 5
    successful_jobs_history_limit = 1
    schedule                      = "0 0 * * *"
    job_template {
      metadata {}
      spec {
        backoff_limit              = 2
        ttl_seconds_after_finished = 600
        template {
          metadata {}
          spec {
            container {
              name    = "hop-metrics"
              image   = "${aws_ecr_repository.hop-metrics.repository_url}:2.0.2"
              env {
                name  = "KAFKA_SECRET"
                value = "kafka-admin-credential"
              }
              env {
                name  = "INFLUX_SECRET"
                value = "hop-metrics-influxDB"
              }
              env {
                name  = "KAFKA_URL"
                value = "kafka://kafka.scimma.org"
              }
              env {
                name  = "AWS_REGION"
                value = "us-west-2"
              }
              env {
                name  = "DATA_SOURCE"
                value = "prod"
              }
            }
            service_account_name = lower("${var.namePrefix}-hop-metrics")
          }
        }
      }
    }
  }
}

// Step 1: Make a service account
resource "kubernetes_service_account" "hop-metrics-account" {
  metadata {
    name = lower("${var.namePrefix}-hop-metrics")
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.hop-metrics-app.arn
    }
  }

  automount_service_account_token = true
}

resource "kubernetes_secret" "hop-metrics-account" {
  metadata {
       name = format("%s%s",lower("${var.namePrefix}-hop-metrics"),"-token")
       annotations = {
         "kubernetes.io/service-account.name" = lower("${var.namePrefix}-hop-metrics")
         }
  }
  
  type = "kubernetes.io/service-account-token"
}

// Step 2: Make a role
resource "aws_iam_role" "hop-metrics-app" {
  name = lower("${var.namePrefix}-hop-metrics")

  assume_role_policy   = data.aws_iam_policy_document.hop_metrics_permit_kubernetes_assume_role.json
  permissions_boundary = "arn:aws:iam::585193511743:policy/NoIAM"
}

// Step 3: Permit the cluster (which should already exist) to assume the role
data "aws_eks_cluster" "hop-metrics-cluster" {
  name = "hopProdEksCluster"
}

data "aws_caller_identity" "hop-metrics-current" {}

locals {
  # Trim the https:// prefix from the OIDC issuer value to get an issuer
  # identifier. This is just the format that AWS expects.
  hop_metrics_oidc_issuer_id = replace(data.aws_eks_cluster.hop-metrics-cluster.identity.0.oidc.0.issuer, "https://", "")
  hop_metrics_oidc_arn       = "arn:aws:iam::${data.aws_caller_identity.hop-metrics-current.account_id}:oidc-provider/${local.hop_metrics_oidc_issuer_id}"
}

data "aws_iam_policy_document" "hop_metrics_permit_kubernetes_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.hop_metrics_oidc_arn]
    }
  }
}

// Step 4: Attach IAM policies to the role we created.
resource "aws_iam_policy" "hop-metrics-policy" {
  name   = lower("${var.namePrefix}-hop-metrics")
  policy = data.aws_iam_policy_document.hop-metrics.json
}

resource "aws_iam_role_policy_attachment" "hop-metrics-attachment" {
  policy_arn = aws_iam_policy.hop-metrics-policy.arn
  role       = aws_iam_role.hop-metrics-app.name
}

# Policy which grants the permissions used by the scimma web server
data "aws_iam_policy_document" "hop-metrics" {
  statement {
    sid     = "PermitReadingSecrets"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      "arn:aws:secretsmanager:us-west-2:585193511743:secret:hop-metrics-influxDB-2B5lwJ",
      "arn:aws:secretsmanager:us-west-2:585193511743:secret:kafka-admin-credential-sIqSpa",
    ]
  }
}