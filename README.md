# 🔍 Newsletter System Monitoring Service

Background monitoring daemon for Newsletter System Worker and Publisher processes.

## 🎯 Purpose

Monitors:
- **Worker Process:** 4x daily (6am, 12pm, 6pm, 12am UTC)
- **Publisher Process:** 1x daily (8am UTC)

## 🚀 Deployment

Deploy to Railway and set environment variables from your main Flask API project.

## 📊 Features

- Process schedule monitoring
- Automatic failure detection
- Email alerts via Mailgun
- API status updates
- Health monitoring

## 🔧 Environment Variables

Copy all environment variables from your main COR Flask API Railway project, including:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- MONITORING_ENABLED=true
- EMAIL_ALERTS_ENABLED=true
- Mailgun SMTP settings
- All other Flask API environment variables

