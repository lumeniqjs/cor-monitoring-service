#!/usr/bin/env python3
"""
Newsletter System Monitoring Service
Monitors Worker and Publisher services, sends alerts, tracks health
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
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NewsletterMonitoringService:
    """Monitoring service for Newsletter System"""
    
    def __init__(self):
        """Initialize monitoring service"""
        logger.info("🚀 Newsletter Monitoring Service Starting")
        self.load_config()
        self.setup_database()
        self.last_alert_times = {}
        
    def load_config(self):
        """Load configuration from environment variables"""
        logger.info("⚙️ Loading configuration...")
        
        # Database
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Missing Supabase configuration")
        
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
    
    def setup_database(self):
        """Setup Supabase database connection"""
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info("✅ Supabase client initialized")
            
            # Test connection
            test_result = self.supabase.table('newsletters').select('uuid').limit(1).execute()
            logger.info("✅ Database connection verified")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup database: {e}")
            raise
    
    def check_worker_health(self):
        """Check worker service health"""
        logger.info("🤖 Checking Worker Service health...")
        
        try:
            # Get recent worker runs (last 2 hours)
            cutoff_time = datetime.utcnow() - timedelta(hours=2)
            cutoff_str = cutoff_time.isoformat() + 'Z'
            
            recent_runs = self.supabase.table('worker_runs').select(
                'id, worker_id, status, started_at, completed_at, tasks_processed, tasks_failed'
            ).gte('started_at', cutoff_str).order('started_at', desc=True).execute()
            
            # Get recent workers
            recent_workers = self.supabase.table('workers').select(
                'id, worker_id, status, registered_at, metadata'
            ).gte('registered_at', cutoff_str).order('registered_at', desc=True).execute()
            
            # Analyze health
            health_status = {
                'service': 'worker',
                'status': 'healthy',
                'recent_runs': len(recent_runs.data) if recent_runs.data else 0,
                'recent_workers': len(recent_workers.data) if recent_workers.data else 0,
                'success_rate': 0,
                'last_run': None,
                'issues': []
            }
            
            if recent_runs.data:
                successful_runs = sum(1 for run in recent_runs.data if run['status'] == 'completed')
                health_status['success_rate'] = (successful_runs / len(recent_runs.data)) * 100
                health_status['last_run'] = recent_runs.data[0]['started_at']
                
                # Check for failures
                if health_status['success_rate'] < 80:
                    health_status['status'] = 'degraded'
                    health_status['issues'].append(f"Low success rate: {health_status['success_rate']:.1f}%")
            else:
                health_status['status'] = 'inactive'
                health_status['issues'].append("No recent worker runs found")
            
            # Check schedule adherence
            schedule_issues = self.check_worker_schedule()
            if schedule_issues:
                health_status['issues'].extend(schedule_issues)
                if health_status['status'] == 'healthy':
                    health_status['status'] = 'degraded'
            
            logger.info(f"🤖 Worker health: {health_status['status']} ({health_status['success_rate']:.1f}% success)")
            return health_status
            
        except Exception as e:
            logger.error(f"❌ Worker health check failed: {e}")
            return {
                'service': 'worker',
                'status': 'error',
                'error': str(e),
                'issues': [f"Health check failed: {e}"]
            }
    
    def check_publisher_health(self):
        """Check publisher service health"""
        logger.info("📰 Checking Publisher Service health...")
        
        try:
            # Get recent newsletters (last 24 hours)
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            cutoff_str = cutoff_time.isoformat() + 'Z'
            
            recent_newsletters = self.supabase.table('newsletters').select(
                'uuid, title, generated_at, vertical_id'
            ).not_.like('title', 'CONFIG_%').not_.like('title', 'PROMPT_%').gte('generated_at', cutoff_str).order('generated_at', desc=True).execute()
            
            # Analyze health
            health_status = {
                'service': 'publisher',
                'status': 'healthy',
                'recent_newsletters': len(recent_newsletters.data) if recent_newsletters.data else 0,
                'last_generation': None,
                'issues': []
            }
            
            if recent_newsletters.data:
                health_status['last_generation'] = recent_newsletters.data[0]['generated_at']
                
                # Check if generation is recent enough
                if health_status['last_generation']:
                    last_gen_time = datetime.fromisoformat(health_status['last_generation'].replace('Z', '+00:00'))
                    hours_since_last = (datetime.utcnow().replace(tzinfo=last_gen_time.tzinfo) - last_gen_time).total_seconds() / 3600
                    
                    if hours_since_last > 25:  # Should generate daily
                        health_status['status'] = 'degraded'
                        health_status['issues'].append(f"No generation in {hours_since_last:.1f} hours")
                else:
                    health_status['status'] = 'degraded'
                    health_status['issues'].append("Recent newsletters have no generation timestamp")
            else:
                health_status['status'] = 'inactive'
                health_status['issues'].append("No recent newsletter generation")
            
            # Check schedule adherence
            schedule_issues = self.check_publisher_schedule()
            if schedule_issues:
                health_status['issues'].extend(schedule_issues)
                if health_status['status'] == 'healthy':
                    health_status['status'] = 'degraded'
            
            logger.info(f"📰 Publisher health: {health_status['status']} ({health_status['recent_newsletters']} recent)")
            return health_status
            
        except Exception as e:
            logger.error(f"❌ Publisher health check failed: {e}")
            return {
                'service': 'publisher',
                'status': 'error',
                'error': str(e),
                'issues': [f"Health check failed: {e}"]
            }
    
    def check_worker_schedule(self):
        """Check if worker is running on schedule"""
        issues = []
        
        try:
            now = datetime.utcnow()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Check each scheduled time
            for scheduled_time in self.worker_schedule['times']:
                hour, minute = map(int, scheduled_time.split(':'))
                scheduled_datetime = today_start.replace(hour=hour, minute=minute)
                
                # If scheduled time has passed, check if worker ran
                if now > scheduled_datetime + timedelta(minutes=self.worker_schedule['max_delay_minutes']):
                    # Look for worker runs around this time
                    window_start = scheduled_datetime - timedelta(minutes=15)
                    window_end = scheduled_datetime + timedelta(minutes=self.worker_schedule['max_delay_minutes'])
                    
                    runs_in_window = self.supabase.table('worker_runs').select('id').gte(
                        'started_at', window_start.isoformat() + 'Z'
                    ).lte('started_at', window_end.isoformat() + 'Z').execute()
                    
                    if not runs_in_window.data:
                        issues.append(f"Missed scheduled run at {scheduled_time} UTC")
            
        except Exception as e:
            issues.append(f"Schedule check failed: {e}")
        
        return issues
    
    def check_publisher_schedule(self):
        """Check if publisher is running on schedule"""
        issues = []
        
        try:
            now = datetime.utcnow()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Check scheduled time (8:00 UTC)
            scheduled_time = self.publisher_schedule['times'][0]
            hour, minute = map(int, scheduled_time.split(':'))
            scheduled_datetime = today_start.replace(hour=hour, minute=minute)
            
            # If scheduled time has passed, check if publisher ran
            if now > scheduled_datetime + timedelta(minutes=self.publisher_schedule['max_delay_minutes']):
                # Look for newsletter generation around this time
                window_start = scheduled_datetime - timedelta(minutes=30)
                window_end = scheduled_datetime + timedelta(minutes=self.publisher_schedule['max_delay_minutes'])
                
                newsletters_in_window = self.supabase.table('newsletters').select('uuid').not_.like(
                    'title', 'CONFIG_%'
                ).not_.like('title', 'PROMPT_%').gte(
                    'generated_at', window_start.isoformat() + 'Z'
                ).lte('generated_at', window_end.isoformat() + 'Z').execute()
                
                if not newsletters_in_window.data:
                    issues.append(f"Missed scheduled generation at {scheduled_time} UTC")
        
        except Exception as e:
            issues.append(f"Schedule check failed: {e}")
        
        return issues
    
    def record_monitoring_event(self, service_name, status, metadata=None):
        """Record monitoring event in database"""
        try:
            monitoring_data = {
                'service_name': service_name,
                'status': status,
                'last_check': datetime.utcnow().isoformat() + 'Z',
                'metadata': metadata or {}
            }
            
            self.supabase.table('process_monitoring').insert(monitoring_data).execute()
            logger.info(f"📊 Recorded monitoring event: {service_name} - {status}")
            
        except Exception as e:
            logger.error(f"❌ Failed to record monitoring event: {e}")
    
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
Newsletter System Monitoring Service
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
            # Check worker health
            worker_health = self.check_worker_health()
            self.record_monitoring_event('worker', worker_health['status'], worker_health)
            
            if worker_health['status'] in ['degraded', 'error', 'inactive']:
                issues_text = '\n'.join(worker_health.get('issues', []))
                self.send_alert(
                    f"Worker Service {worker_health['status'].title()}",
                    f"Worker service issues detected:\n\n{issues_text}",
                    'worker'
                )
            
            # Check publisher health
            publisher_health = self.check_publisher_health()
            self.record_monitoring_event('publisher', publisher_health['status'], publisher_health)
            
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
        logger.info("🚀 Newsletter Monitoring Service started")
        logger.info(f"⚙️ Health check interval: {self.health_check_interval}s")
        logger.info(f"📧 Email alerts: {'enabled' if self.email_alerts_enabled else 'disabled'}")
        
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

