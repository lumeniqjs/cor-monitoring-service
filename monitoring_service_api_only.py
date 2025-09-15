#!/usr/bin/env python3
"""
Newsletter System Monitoring Service - API Only
Monitors Worker and Publisher services via Flask API endpoints ONLY
NO DIRECT DATABASE ACCESS - Follows Separation of Concerns
"""

import os
import time
import json
import logging
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NewsletterMonitoringService:
    """
    Monitoring service for Newsletter System
    FOLLOWS SEPARATION OF CONCERNS - API ACCESS ONLY
    """
    
    def __init__(self):
        """Initialize monitoring service"""
        logger.info("🚀 Newsletter Monitoring Service Starting (API Only)")
        self.load_config()
        self.last_alert_times = {}
        self.validate_configuration()
        
    def load_config(self):
        """Load configuration from environment variables"""
        logger.info("⚙️ Loading configuration...")
        
        # API Access (REQUIRED)
        self.flask_api_url = os.getenv('FLASK_API_URL', 'https://api.contentonrails.com')
        self.flask_api_key = os.getenv('FLASK_API_KEY')
        
        # Ensure no database access (COMPLIANCE CHECK)
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        qdrant_url = os.getenv('QDRANT_URL')
        
        if supabase_url or supabase_key or qdrant_url:
            logger.warning("⚠️ ARCHITECTURE VIOLATION: Database credentials detected!")
            logger.warning("⚠️ Monitoring Service should use API endpoints ONLY")
            logger.warning("⚠️ Remove SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, QDRANT_URL")
        
        # Monitoring settings
        self.monitoring_enabled = os.getenv('MONITORING_ENABLED', 'true').lower() == 'true'
        self.health_check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', 300))  # 5 minutes
        self.schedule_check_interval = int(os.getenv('SCHEDULE_CHECK_INTERVAL', 600))  # 10 minutes
        self.alert_cooldown_minutes = int(os.getenv('ALERT_COOLDOWN_MINUTES', 30))
        self.max_consecutive_failures = int(os.getenv('MAX_CONSECUTIVE_FAILURES', 3))
        
        # Email alerts
        self.email_alerts_enabled = os.getenv('EMAIL_ALERTS_ENABLED', 'true').lower() == 'true'
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.mailgun.org')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
        self.smtp_username = os.getenv('MAILGUN_SMTP_USERNAME')
        self.smtp_password = os.getenv('MAILGUN_SMTP_PASSWORD')
        self.alert_email_from = os.getenv('ALERT_EMAIL_FROM')
        self.alert_email_to = os.getenv('ALERT_EMAIL_TO')
        self.alert_subject_prefix = os.getenv('ALERT_EMAIL_SUBJECT_PREFIX', '[Newsletter System Alert]')
        
        # Service schedules
        self.worker_schedule = {
            'frequency': '4x_daily',
            'times': ['06:00', '12:00', '18:00', '00:00'],  # UTC
            'max_delay_minutes': 30
        }
        
        self.publisher_schedule = {
            'frequency': '1x_daily', 
            'times': ['08:00'],  # UTC
            'max_delay_minutes': 60
        }
        
        logger.info("✅ Configuration loaded")
    
    def validate_configuration(self):
        """Validate configuration follows architectural guidelines"""
        logger.info("🔍 Validating architectural compliance...")
        
        # Check API access
        if not self.flask_api_url:
            raise ValueError("FLASK_API_URL is required for monitoring service")
        
        # Check email configuration
        if self.email_alerts_enabled:
            required_email_vars = [
                'MAILGUN_SMTP_USERNAME', 'MAILGUN_SMTP_PASSWORD',
                'ALERT_EMAIL_FROM', 'ALERT_EMAIL_TO'
            ]
            missing_vars = [var for var in required_email_vars if not os.getenv(var)]
            if missing_vars:
                logger.warning(f"⚠️ Missing email configuration: {missing_vars}")
                self.email_alerts_enabled = False
        
        logger.info("✅ Configuration validation complete")
    
    def make_api_request(self, endpoint, method='GET', data=None):
        """Make API request to Flask API"""
        url = f"{self.flask_api_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        if self.flask_api_key:
            headers['Authorization'] = f"Bearer {self.flask_api_key}"
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=30)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed: {url} - {e}")
            return None
    
    def check_worker_health(self):
        """Check worker service health via API"""
        logger.info("🤖 Checking Worker Service health via API...")
        
        try:
            # Get worker status from API
            worker_status = self.make_api_request('/api/v1/monitoring/worker/status')
            
            if not worker_status:
                return {
                    'service': 'worker',
                    'status': 'error',
                    'issues': ['Failed to get worker status from API'],
                    'api_accessible': False
                }
            
            # Analyze health based on API response
            health_status = {
                'service': 'worker',
                'status': worker_status.get('status', 'unknown'),
                'recent_runs': worker_status.get('recent_runs', 0),
                'success_rate': worker_status.get('success_rate', 0),
                'last_run': worker_status.get('last_run'),
                'issues': worker_status.get('issues', []),
                'api_accessible': True
            }
            
            # Additional health checks
            if health_status['success_rate'] < 80:
                health_status['status'] = 'degraded'
                health_status['issues'].append(f"Low success rate: {health_status['success_rate']:.1f}%")
            
            if health_status['recent_runs'] == 0:
                health_status['status'] = 'inactive'
                health_status['issues'].append("No recent worker runs")
            
            logger.info(f"🤖 Worker health: {health_status['status']} ({health_status['success_rate']:.1f}% success)")
            return health_status
            
        except Exception as e:
            logger.error(f"❌ Worker health check failed: {e}")
            return {
                'service': 'worker',
                'status': 'error',
                'error': str(e),
                'issues': [f"Health check failed: {e}"],
                'api_accessible': False
            }
    
    def check_publisher_health(self):
        """Check publisher service health via API"""
        logger.info("📰 Checking Publisher Service health via API...")
        
        try:
            # Get publisher status from API
            publisher_status = self.make_api_request('/api/v1/monitoring/publisher/status')
            
            if not publisher_status:
                return {
                    'service': 'publisher',
                    'status': 'error',
                    'issues': ['Failed to get publisher status from API'],
                    'api_accessible': False
                }
            
            # Analyze health based on API response
            health_status = {
                'service': 'publisher',
                'status': publisher_status.get('status', 'unknown'),
                'recent_newsletters': publisher_status.get('recent_newsletters', 0),
                'last_generation': publisher_status.get('last_generation'),
                'issues': publisher_status.get('issues', []),
                'api_accessible': True
            }
            
            # Additional health checks
            if health_status['last_generation']:
                try:
                    last_gen_time = datetime.fromisoformat(health_status['last_generation'].replace('Z', '+00:00'))
                    hours_since_last = (datetime.utcnow().replace(tzinfo=last_gen_time.tzinfo) - last_gen_time).total_seconds() / 3600
                    
                    if hours_since_last > 25:  # Should generate daily
                        health_status['status'] = 'degraded'
                        health_status['issues'].append(f"No generation in {hours_since_last:.1f} hours")
                except Exception as e:
                    health_status['issues'].append(f"Invalid generation timestamp: {e}")
            
            if health_status['recent_newsletters'] == 0:
                health_status['status'] = 'inactive'
                health_status['issues'].append("No recent newsletter generation")
            
            logger.info(f"📰 Publisher health: {health_status['status']} ({health_status['recent_newsletters']} recent)")
            return health_status
            
        except Exception as e:
            logger.error(f"❌ Publisher health check failed: {e}")
            return {
                'service': 'publisher',
                'status': 'error',
                'error': str(e),
                'issues': [f"Health check failed: {e}"],
                'api_accessible': False
            }
    
    def check_overall_system_health(self):
        """Check overall system health via API"""
        logger.info("🔍 Checking Overall System health via API...")
        
        try:
            # Get overall system status from API
            system_status = self.make_api_request('/api/v1/monitoring/status')
            
            if not system_status:
                return {
                    'status': 'error',
                    'issues': ['Failed to get system status from API'],
                    'api_accessible': False
                }
            
            return {
                'status': system_status.get('status', 'unknown'),
                'components': system_status.get('components', {}),
                'api_accessible': True,
                'timestamp': system_status.get('timestamp')
            }
            
        except Exception as e:
            logger.error(f"❌ System health check failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'issues': [f"System health check failed: {e}"],
                'api_accessible': False
            }
    
    def record_monitoring_heartbeat(self):
        """Record monitoring service heartbeat via API"""
        try:
            heartbeat_data = {
                'service': 'monitoring',
                'status': 'active',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'version': '2.0.0-api-only'
            }
            
            result = self.make_api_request('/api/v1/monitoring/heartbeat', 'POST', heartbeat_data)
            
            if result:
                logger.info("💓 Monitoring heartbeat recorded")
            else:
                logger.warning("⚠️ Failed to record monitoring heartbeat")
                
        except Exception as e:
            logger.error(f"❌ Failed to record heartbeat: {e}")
    
    def send_alert(self, subject, message, service_name):
        """Send email alert"""
        if not self.email_alerts_enabled:
            logger.info("📧 Email alerts disabled, skipping alert")
            return
        
        # Check cooldown
        alert_key = f"{service_name}_{subject}"
        now = datetime.utcnow()
        
        if alert_key in self.last_alert_times:
            time_since_last = (now - self.last_alert_times[alert_key]).total_seconds() / 60
            if time_since_last < self.alert_cooldown_minutes:
                logger.info(f"📧 Alert cooldown active for {alert_key}, skipping")
                return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.alert_email_from
            msg['To'] = self.alert_email_to
            msg['Subject'] = f"{self.alert_subject_prefix} {subject}"
            
            body = f"""
Newsletter System Alert

Service: {service_name}
Time: {now.isoformat()} UTC
Issue: {subject}

Details:
{message}

---
Newsletter System Monitoring Service (API Only)
Architecture: Separation of Concerns Compliant
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            if self.smtp_use_tls:
                server.starttls()
            server.login(self.smtp_username, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            self.last_alert_times[alert_key] = now
            logger.info(f"📧 Alert sent: {subject}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send alert: {e}")
    
    def run_monitoring_cycle(self):
        """Run one complete monitoring cycle"""
        logger.info("🔄 Starting monitoring cycle...")
        
        try:
            # Record heartbeat
            self.record_monitoring_heartbeat()
            
            # Check overall system health
            system_health = self.check_overall_system_health()
            
            if not system_health.get('api_accessible', False):
                self.send_alert(
                    "API Connection Failed",
                    f"Cannot connect to Flask API at {self.flask_api_url}",
                    'monitoring'
                )
                return
            
            # Check worker health
            worker_health = self.check_worker_health()
            
            if worker_health['status'] in ['degraded', 'error', 'inactive']:
                issues_text = '\n'.join(worker_health.get('issues', []))
                self.send_alert(
                    f"Worker Service {worker_health['status'].title()}",
                    f"Worker service issues detected:\n\n{issues_text}",
                    'worker'
                )
            
            # Check publisher health
            publisher_health = self.check_publisher_health()
            
            if publisher_health['status'] in ['degraded', 'error', 'inactive']:
                issues_text = '\n'.join(publisher_health.get('issues', []))
                self.send_alert(
                    f"Publisher Service {publisher_health['status'].title()}",
                    f"Publisher service issues detected:\n\n{issues_text}",
                    'publisher'
                )
            
            # Log summary
            logger.info(f"📊 Monitoring cycle complete - Worker: {worker_health['status']}, Publisher: {publisher_health['status']}")
            
        except Exception as e:
            logger.error(f"❌ Monitoring cycle failed: {e}")
            self.send_alert("Monitoring System Error", f"Monitoring cycle failed: {e}", "monitoring")
    
    def run(self):
        """Main monitoring loop"""
        logger.info("🚀 Newsletter Monitoring Service started (API Only)")
        logger.info(f"🔗 Flask API URL: {self.flask_api_url}")
        logger.info(f"⚙️ Health check interval: {self.health_check_interval}s")
        logger.info(f"📧 Email alerts: {'enabled' if self.email_alerts_enabled else 'disabled'}")
        logger.info("🏗️ Architecture: Separation of Concerns Compliant")
        
        if not self.monitoring_enabled:
            logger.warning("⚠️ Monitoring is disabled")
            return
        
        try:
            while True:
                self.run_monitoring_cycle()
                
                logger.info(f"😴 Sleeping for {self.health_check_interval} seconds...")
                time.sleep(self.health_check_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Monitoring service stopped by user")
        except Exception as e:
            logger.error(f"💥 Monitoring service crashed: {e}")
            raise


def main():
    """Main entry point"""
    try:
        service = NewsletterMonitoringService()
        service.run()
    except Exception as e:
        logger.error(f"💥 Failed to start monitoring service: {e}")
        exit(1)


if __name__ == "__main__":
    main()

