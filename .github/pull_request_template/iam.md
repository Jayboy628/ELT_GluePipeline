# 🔐 IAM Task: Update to IAM Roles and Policies

## ✨ Summary
<!-- What is being changed? -->
Updates IAM roles and policies to meet least-privilege and task-specific access for Glue jobs and GitHub CI/CD.

## 📂 Files Changed
- `02_iam.yml`: Updated GlueJobRole with scoped S3/Secrets permissions
- `deploy_pipeline.sh`: Updated parameter references for role ARN injection

## 🔍 Rationale
- Security best practice: minimize access scope
- Align roles with job-specific functions
- Prepares for environment isolation

## ✅ Checklist
- [ ] IAM changes tested via `cloudformation deploy`
- [ ] CI/CD workflow verified on push
- [ ] Documentation updated if needed

## 🔗 Related Issues / Tasks
- Resolves #IAM-001

