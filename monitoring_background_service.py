#!/usr/bin/env python3
"""
Newsletter System Background Monitoring Service
Monitors Worker (4x daily) and Publisher (1x daily) processes
"""

import os
import time
import json
import logging
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MonitoringService:
    def __init__(self):
        """Initialize monitoring service with environment variables"""
        self.load_config()
        self.setup_schedules()
        
    def load_config(self):
        """Load configuration from environment variables"""
        # Database
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        # Monitoring
        self.monitoring_enabled = os.getenv('MONITORING_ENABLED', 'true').lower() == 'true'
        self.health_check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', 300))
        self.email_alerts_enabled = os.getenv('EMAIL_ALERTS_ENABLED', 'true').lower() == 'true'
        
        # Email configuration
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.mailgun.org')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
        self.smtp_username = os.getenv('MAILGUN_SMTP_USERNAME')
        self.smtp_password = os.getenv('MAILGUN_SMTP_PASSWORD')
        self.alert_email_from = os.getenv('ALERT_EMAIL_FROM')
        self.alert_email_to = os.getenv('ALERT_EMAIL_TO')
        self.alert_subject_prefix = os.getenv('ALERT_EMAIL_SUBJECT_PREFIX', '[Newsletter System Alert]')
        
        # API connection
        self.flask_api_url = os.getenv('FLASK_API_URL', 'https://api.contentonrails.com')
        self.flask_api_key = os.getenv('FLASK_API_KEY')
        
        logger.info("✅ Configuration loaded successfully")
        
    def setup_schedules(self):
        """Set up monitoring schedules"""
        # Worker: 4x daily (6am, 12pm, 6pm, 12am UTC)
        self.worker_schedule = [6, 12, 18, 0]  # Hours in UTC
        self.worker_tolerance = 30  # minutes
        
        # Publisher: 1x daily (8am UTC)  
        self.publisher_schedule = [8]  # Hours in UTC
        self.publisher_tolerance = 60  # minutes
        
        logger.info("✅ Monitoring schedules configured")
        logger.info(f"   Worker: {len(self.worker_schedule)}x daily at {self.worker_schedule} UTC")
        logger.info(f"   Publisher: {len(self.publisher_schedule)}x daily at {self.publisher_schedule} UTC")
        
    def send_email_alert(self, subject, message):
        """Send email alert via Mailgun SMTP"""
        if not self.email_alerts_enabled:
            logger.info("📧 Email alerts disabled - skipping notification")
            return
            
        if not all([self.smtp_username, self.smtp_password, self.alert_email_from, self.alert_email_to]):
            logger.error("❌ Email configuration incomplete - cannot send alert")
            return
            
        try:
            msg = MimeMultipart()
            msg['From'] = self.alert_email_from
            msg['To'] = self.alert_email_to
            msg['Subject'] = f"{self.alert_subject_prefix} {subject}"
            
            msg.attach(MimeText(message, 'plain'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            if self.smtp_use_tls:
                server.starttls()
            server.login(self.smtp_username, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"📧 Email alert sent: {subject}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send email alert: {e}")
            
    def update_monitoring_status(self, process_type, status_data):
        """Update monitoring status via Flask API"""
        try:
            url = f"{self.flask_api_url}/api/v1/monitoring/update"
            headers = {'Content-Type': 'application/json'}
            if self.flask_api_key:
                headers['Authorization'] = f"Bearer {self.flask_api_key}"
                
            payload = {
                'process_type': process_type,
                'status': status_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Updated {process_type} status via API")
            else:
                logger.warning(f"⚠️ API status update failed: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Failed to update status via API: {e}")
            
    def check_process_schedule(self, process_type, schedule_hours, tolerance_minutes):
        """Check if a process is running on schedule"""
        now = datetime.utcnow()
        current_hour = now.hour
        current_minute = now.minute
        
        # Check if we're within tolerance of any scheduled time
        for scheduled_hour in schedule_hours:
            # Calculate time difference
            scheduled_time = now.replace(hour=scheduled_hour, minute=0, second=0, microsecond=0)
            
            # Handle day boundary
            if scheduled_hour < current_hour or (scheduled_hour == current_hour and current_minute > tolerance_minutes):
                # Check if we missed today's run
                time_diff = now - scheduled_time
                if time_diff.total_seconds() / 60 > tolerance_minutes:
                    return {
                        'status': 'overdue',
                        'scheduled_time': scheduled_time.isoformat(),
                        'minutes_overdue': int(time_diff.total_seconds() / 60),
                        'healthy': False
                    }
            elif scheduled_hour == current_hour and current_minute <= tolerance_minutes:
                # Currently in scheduled window
                return {
                    'status': 'on_schedule',
                    'scheduled_time': scheduled_time.isoformat(),
                    'healthy': True
                }
                
        return {
            'status': 'waiting',
            'next_scheduled': self.get_next_scheduled_time(schedule_hours).isoformat(),
            'healthy': True
        }
        
    def get_next_scheduled_time(self, schedule_hours):
        """Get the next scheduled time for a process"""
        now = datetime.utcnow()
        current_hour = now.hour
        
        # Find next scheduled hour today
        for hour in sorted(schedule_hours):
            if hour > current_hour:
                return now.replace(hour=hour, minute=0, second=0, microsecond=0)
                
        # Next run is tomorrow at first scheduled hour
        tomorrow = now + timedelta(days=1)
        return tomorrow.replace(hour=min(schedule_hours), minute=0, second=0, microsecond=0)
        
    def monitor_processes(self):
        """Main monitoring loop"""
        logger.info("🔍 Starting process monitoring cycle")
        
        # Check Worker process
        worker_status = self.check_process_schedule('worker', self.worker_schedule, self.worker_tolerance)
        logger.info(f"🤖 Worker status: {worker_status['status']}")
        
        if worker_status['status'] == 'overdue':
            subject = "Worker Process Overdue"
            message = f"""The Worker process has not completed its scheduled run.

Details:
- Process: Worker
- Expected: 4 times daily ({', '.join(map(str, self.worker_schedule))} UTC)
- Status: OVERDUE ({worker_status['minutes_overdue']} minutes late)
- Scheduled Time: {worker_status['scheduled_time']}
- Next Action: Automatic retry in 5 minutes

This is an automated alert from the Newsletter System monitoring service."""
            
            self.send_email_alert(subject, message)
            
        self.update_monitoring_status('worker', worker_status)
        
        # Check Publisher process
        publisher_status = self.check_process_schedule('publisher', self.publisher_schedule, self.publisher_tolerance)
        logger.info(f"📰 Publisher status: {publisher_status['status']}")
        
        if publisher_status['status'] == 'overdue':
            subject = "Publisher Process Overdue"
            message = f"""The Publisher process has not completed its scheduled run.

Details:
- Process: Publisher
- Expected: 1 time daily ({', '.join(map(str, self.publisher_schedule))} UTC)
- Status: OVERDUE ({publisher_status['minutes_overdue']} minutes late)
- Scheduled Time: {publisher_status['scheduled_time']}
- Next Action: Automatic retry in 10 minutes

This is an automated alert from the Newsletter System monitoring service."""
            
            self.send_email_alert(subject, message)
            
        self.update_monitoring_status('publisher', publisher_status)
        
        logger.info("✅ Monitoring cycle completed")
        
    def run(self):
        """Main monitoring service loop"""
        logger.info("🚀 Newsletter System Monitoring Service Starting")
        logger.info(f"   Monitoring Enabled: {self.monitoring_enabled}")
        logger.info(f"   Email Alerts: {self.email_alerts_enabled}")
        logger.info(f"   Health Check Interval: {self.health_check_interval} seconds")
        
        # Send startup notification
        if self.email_alerts_enabled:
            self.send_email_alert(
                "Monitoring Service Started",
                f"""Newsletter System monitoring service has started successfully.

Configuration:
- Worker Monitoring: 4x daily at {', '.join(map(str, self.worker_schedule))} UTC
- Publisher Monitoring: 1x daily at {', '.join(map(str, self.publisher_schedule))} UTC
- Health Check Interval: {self.health_check_interval} seconds
- Email Alerts: Enabled

The service will now monitor your newsletter processes and send alerts for any issues.

Started at: {datetime.utcnow().isoformat()} UTC"""
            )
            
        # Main monitoring loop
        while True:
            try:
                if self.monitoring_enabled:
                    self.monitor_processes()
                else:
                    logger.info("⏸️ Monitoring disabled - skipping checks")
                    
                logger.info(f"😴 Sleeping for {self.health_check_interval} seconds...")
                time.sleep(self.health_check_interval)
                
            except KeyboardInterrupt:
                logger.info("🛑 Monitoring service stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
                
        logger.info("👋 Newsletter System Monitoring Service Stopped")

if __name__ == "__main__":
    service = MonitoringService()
    service.run()

