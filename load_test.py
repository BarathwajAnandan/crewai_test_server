#!/usr/bin/env python3
"""
load_test.py - Performance testing script for SecureBank Enterprise API
Can be run both locally and in Docker containers
"""

import asyncio
import aiohttp
import time
import random
import os
import json
import sys
from datetime import datetime

class SecureBankLoadTester:
    def __init__(self):
        # Support both local and Docker environments
        self.server_url = os.getenv('SERVER_URL', 'http://localhost:8000')
        # self.fixed_url = os.getenv('FIXED_SERVER_URL', 'http://localhost:8001')

        # If running in Docker, use container networking
        if 'DOCKER_CONTAINER' in os.environ or 'HOSTNAME' in os.environ:
            self.server_url = os.getenv('SERVER_URL', 'http://securebank-server:8000')
            # self.fixed_url = os.getenv('FIXED_SERVER_URL', 'http://fixed-server:8000')

        self.session_timeout = aiohttp.ClientTimeout(total=60, connect=15)

    async def make_request(self, session, url: str, data=None, method="POST", timeout=15):
        """Make HTTP request with proper error handling"""
        try:
            if method.upper() == "GET":
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        print(f"❌ Request failed: {response.status} - {url}")
                        return None
            else:  # POST
                if data:
                    async with session.post(url, json=data, timeout=timeout) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            print(f"❌ Request failed: {response.status} - {url}")
                            return None
                else:
                    async with session.post(url, timeout=timeout) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            print(f"❌ Request failed: {response.status} - {url}")
                            return None
        except asyncio.TimeoutError:
            print(f"⏱️  Request timeout: {url}")
            return None
        except Exception as e:
            print(f"💥 Request error for {url}: {e}")
            return None

    async def test_server_connectivity(self, server_url: str):
        """Test if server is reachable"""
        try:
            async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
                result = await self.make_request(session, f"{server_url}/", method="GET", timeout=10)
                if result:
                    print(f"✅ Server {server_url} is reachable")
                    return True
                else:
                    print(f"❌ Server {server_url} is not responding")
                    return False
        except Exception as e:
            print(f"❌ Cannot reach {server_url}: {e}")
            return False

    async def get_memory_dump(self, server_url: str):
        """Get memory dump from server"""
        try:
            async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
                result = await self.make_request(session, f"{server_url}/api/v1/system/diagnostics", method="GET", timeout=45)
                return result
        except Exception as e:
            print(f"Failed to get memory dump from {server_url}: {e}")
            return None

    async def get_thread_dump(self, server_url: str):
        """Get thread dump from server"""
        try:
            async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
                result = await self.make_request(session, f"{server_url}/api/v1/system/concurrency", method="GET", timeout=45)
                return result
        except Exception as e:
            print(f"Failed to get thread dump from {server_url}: {e}")
            return None

    async def simulate_memory_leaks(self, server_url: str, intensity='heavy'):
        """Simulate memory leaks with different intensities"""

        intensities = {
            'light': {'cache_requests': 30, 'sessions': 15, 'background_tasks': 8},
            'medium': {'cache_requests': 50, 'sessions': 25, 'background_tasks': 10},
            'heavy': {'cache_requests': 80, 'sessions': 40, 'background_tasks': 15},
            'extreme': {'cache_requests': 120, 'sessions': 60, 'background_tasks': 20}
        }

        config = intensities.get(intensity, intensities['heavy'])

        print(f"🔥 Starting {intensity} transaction load simulation on {server_url}")
        print(f"   Cache requests: {config['cache_requests']}")
        print(f"   User sessions: {config['sessions']}")
        print(f"   Background tasks: {config['background_tasks']}")

        async with aiohttp.ClientSession(timeout=self.session_timeout) as session:

            # 1. Cache Memory Leak - Process in smaller waves for speed
            print("📦 Processing high-frequency transactions in waves...")
            wave_size = 10  # Process 15 requests per wave for faster execution
            total_waves = (config['cache_requests'] + wave_size - 1) // wave_size

            for wave in range(total_waves):
                wave_start = wave * wave_size
                wave_end = min(wave_start + wave_size, config['cache_requests'])
                print(f"   Wave {wave + 1}/{total_waves}: Processing {wave_end - wave_start} transactions...")

                cache_tasks = []
                for i in range(wave_start, wave_end):
                    data = {
                        "data": f"leak_test_data_{i}_{time.time()}",
                        "iterations": random.randint(1, 5)
                    }
                    task = self.make_request(session, f"{server_url}/api/v1/transactions/process", data)
                    cache_tasks.append(task)

                # Process the entire wave
                await asyncio.gather(*cache_tasks, return_exceptions=True)

                # Check memory after each wave
                exceeded, current_memory = await self.check_memory_threshold(server_url)
                if exceeded:
                    print(f"🚨 MEMORY LEAK DETECTED! Test stopped for safety. Current memory: {current_memory:.1f}MB")
                    return

                # Brief pause between waves for stability
                if wave < total_waves - 1:  # Don't pause after the last wave
                    await asyncio.sleep(0.5)

            print(f"✅ Completed {config['cache_requests']} cache requests")

            # 2. Session Memory Leak - Process in waves
            print("👥 Creating user sessions in waves...")
            session_wave_size = 10  # Smaller waves for faster sessions
            session_total_waves = (config['sessions'] + session_wave_size - 1) // session_wave_size

            for wave in range(session_total_waves):
                wave_start = wave * session_wave_size
                wave_end = min(wave_start + session_wave_size, config['sessions'])
                print(f"   Wave {wave + 1}/{session_total_waves}: Creating {wave_end - wave_start} sessions...")

                session_tasks = []
                for i in range(wave_start, wave_end):
                    user_id = f"load_test_user_{i}_{int(time.time())}"
                    task = self.make_request(session, f"{server_url}/api/v1/accounts/session?user_id={user_id}")
                    session_tasks.append(task)

                # Process the entire wave
                await asyncio.gather(*session_tasks, return_exceptions=True)

                # Check memory after each session wave
                exceeded, current_memory = await self.check_memory_threshold(server_url)
                if exceeded:
                    print(f"🚨 MEMORY LEAK DETECTED! Test stopped for safety. Current memory: {current_memory:.1f}MB")
                    return

                # Brief pause between session waves
                if wave < session_total_waves - 1:
                    await asyncio.sleep(0.3)

            print(f"✅ Completed {config['sessions']} session requests")

            # 3. Background Task Leak - Process in waves
            print("⚙️  Generating regulatory reports in waves...")
            report_wave_size = 5  # Small waves for heavy background tasks
            report_total_waves = (config['background_tasks'] + report_wave_size - 1) // report_wave_size

            for wave in range(report_total_waves):
                wave_start = wave * report_wave_size
                wave_end = min(wave_start + report_wave_size, config['background_tasks'])
                print(f"   Wave {wave + 1}/{report_total_waves}: Generating {wave_end - wave_start} reports...")

                for i in range(wave_start, wave_end):
                    await self.make_request(session, f"{server_url}/api/v1/reports/generate")
                    await asyncio.sleep(0.2)  # Shorter delay between reports

                # Check memory after each report wave
                exceeded, current_memory = await self.check_memory_threshold(server_url)
                if exceeded:
                    print(f"🚨 MEMORY LEAK DETECTED! Test stopped for safety. Current memory: {current_memory:.1f}MB")
                    return

                # Brief pause between report waves
                if wave < report_total_waves - 1:
                    await asyncio.sleep(0.5)

            print(f"✅ Completed {config['background_tasks']} background task requests")

    async def simulate_race_conditions(self, server_url: str, intensity='medium'):
        """Simulate race conditions with concurrent requests"""

        intensities = {
            'light': {'transfers': 20, 'inventory': 15},
            'medium': {'transfers': 50, 'inventory': 30},
            'heavy': {'transfers': 100, 'inventory': 60},
            'extreme': {'transfers': 200, 'inventory': 120}
        }

        config = intensities.get(intensity, intensities['heavy'])

        print(f"⚡ Starting {intensity} concurrent transaction simulation on {server_url}")
        print(f"   Concurrent transfers: {config['transfers']}")
        print(f"   Inventory updates: {config['inventory']}")

        async with aiohttp.ClientSession(timeout=self.session_timeout) as session:

            # 1. Bank Transfer Race Conditions
            print("💰 Processing concurrent wire transfers...")
            transfer_tasks = []

            for i in range(config['transfers']):
                data = {
                    "amount": random.uniform(10.0, 100.0),
                    "from_account": "account_1",
                    "to_account": "account_2"
                }
                task = self.make_request(session, f"{server_url}/api/v1/transfers/execute", data)
                transfer_tasks.append(task)

            # Execute all transfers concurrently to maximize race condition chance
            transfer_results = await asyncio.gather(*transfer_tasks, return_exceptions=True)
            successful_transfers = sum(1 for r in transfer_results
                                     if r and not isinstance(r, Exception)
                                     and isinstance(r, dict))

            print(f"✅ Transfer requests: {successful_transfers}/{config['transfers']} successful")

            # Check memory after transfers
            exceeded, current_memory = await self.check_memory_threshold(server_url)
            if exceeded:
                print(f"🚨 MEMORY LEAK DETECTED! Test stopped for safety. Current memory: {current_memory:.1f}MB")
                return

            # 2. Inventory Race Conditions
            print("📦 Updating portfolio positions concurrently...")
            inventory_tasks = []

            for i in range(config['inventory']):
                data = {
                    "item": f"item_{i % 5}",  # Use same items to increase contention
                    "quantity": random.randint(1, 10)
                }
                task = self.make_request(session, f"{server_url}/api/v1/portfolio/update?item={data['item']}&quantity={data['quantity']}")
                inventory_tasks.append(task)

            # Execute all inventory updates concurrently
            inventory_results = await asyncio.gather(*inventory_tasks, return_exceptions=True)
            successful_inventory = sum(1 for r in inventory_results
                                     if r and not isinstance(r, Exception)
                                     and isinstance(r, dict))

            print(f"✅ Inventory requests: {successful_inventory}/{config['inventory']} successful")

            # Final memory check after race conditions
            exceeded, current_memory = await self.check_memory_threshold(server_url)
            if exceeded:
                print(f"🚨 MEMORY LEAK DETECTED! Test stopped for safety. Current memory: {current_memory:.1f}MB")
                return

    # async def analyze_results(self, server_url: str, server_name: str):
    #     """Analyze server state after load testing"""
    #     print(f"\n🔍 Analyzing {server_name} server results...")

    #     # Get memory dump with retries
    #     memory_dump = None
    #     for attempt in range(3):
    #         print(f"📊 Attempting memory diagnostics (attempt {attempt + 1}/3)...")
    #         memory_dump = await self.get_memory_dump(server_url)
    #         if memory_dump:
    #             break
    #         await asyncio.sleep(2)
    #     if memory_dump:
    #         print(f"📊 Memory Analysis:")
    #         print(f"   Memory Usage: {memory_dump.get('memory_usage_mb', 0):.2f} MB")
    #         print(f"   Transaction Cache Size: {memory_dump.get('transaction_cache_size', 0)}")
    #         print(f"   Active Sessions: {memory_dump.get('active_user_sessions', 0)}")
    #         print(f"   Pending Reports: {memory_dump.get('pending_reports', 0)}")

    #         # Check for memory leak indicators
    #         if memory_dump.get('memory_usage_mb', 0) > 200:
    #             print("   🚨 HIGH MEMORY USAGE - Possible memory leak!")

    #         if memory_dump.get('transaction_cache_size', 0) > 40:
    #             print("   🚨 LARGE TRANSACTION CACHE - Cache not being cleaned up!")

    #         if memory_dump.get('pending_reports', 0) > 10:
    #             print("   🚨 MANY PENDING REPORTS - Report queue leak detected!")
    #     else:
    #         print("⚠️  Could not retrieve memory diagnostics - server may be overloaded")

    #     # Get thread dump with retries
    #     thread_dump = None
    #     for attempt in range(3):
    #         print(f"🧵 Attempting concurrency metrics (attempt {attempt + 1}/3)...")
    #         thread_dump = await self.get_thread_dump(server_url)
    #         if thread_dump:
    #             break
    #         await asyncio.sleep(2)
    #     if thread_dump:
    #         print(f"🧵 Thread Analysis:")
    #         print(f"   Active Threads: {thread_dump.get('active_threads', 0)}")

    #         # Check banking system state for race conditions
    #         banking_state = thread_dump.get('banking_system_state', {})
    #         total_deposits = banking_state.get('total_deposits', 0)
    #         print(f"   Total Deposits: {total_deposits}")

    #         if total_deposits < 0:
    #             print("   🚨 NEGATIVE DEPOSITS - Race condition detected!")

    #         portfolio_positions = banking_state.get('portfolio_positions', 0)
    #         print(f"   Portfolio Positions: {portfolio_positions}")
    #     else:
    #         print("⚠️  Could not retrieve concurrency metrics - server may be overloaded")

    #     # Final status check
    #     if not memory_dump and not thread_dump:
    #         print("🚨 Server appears to be severely impacted by memory issues!")
    #         print("💡 Try running: curl http://localhost:8000/ to test basic connectivity")

    async def run_comprehensive_test(self, target_servers=None, intensity='medium'):
        """Run comprehensive load test on specified servers"""

        if target_servers is None:
            target_servers = [
                (self.server_url, "SecureBank")
                # (self.fixed_url, "Fixed")
            ]

        print("🚀 Starting Comprehensive SecureBank API Load Test")
        print("=" * 60)
        print(f"Intensity: {intensity}")
        print(f"Target servers: {len(target_servers)}")
        print(f"Timestamp: {datetime.now().isoformat()}")

        for server_url, server_name in target_servers:
            print(f"\n{'🔥' if 'buggy' in server_name.lower() else '✅'} Testing {server_name} Server: {server_url}")
            print("-" * 50)

            # Test connectivity
            if not await self.test_server_connectivity(server_url):
                print(f"⏭️  Skipping {server_name} server (not reachable)")
                continue

            # Run load tests directly
            try:
                # Memory leak simulation
                await self.simulate_memory_leaks(server_url, intensity)
                await asyncio.sleep(2)  # Brief pause

                # Race condition simulation
                await self.simulate_race_conditions(server_url, intensity)
                await asyncio.sleep(2)  # Brief pause

                # Quick stabilization
                print("⏳ Waiting for server to stabilize...")
                await asyncio.sleep(2)

                # Analyze and generate report
                await self.analyze_and_generate_report(server_url, server_name)

            except Exception as e:
                print(f"❌ Error testing {server_name} server: {e}")

        print(f"\n🎉 Load test completed at {datetime.now().isoformat()}")
        print("💡 Use the following commands to get detailed diagnostics:")
        for server_url, server_name in target_servers:
            print(f"   {server_name}: curl {server_url}/api/v1/system/diagnostics | jq")

    async def run_single_test(self, server_url: str, test_type: str, intensity='medium'):
        """Run a specific test type on a single server"""

        print(f"🎯 Running {test_type} test on {server_url} (intensity: {intensity})")

        if not await self.test_server_connectivity(server_url):
            print("❌ Server not reachable")
            return

        if test_type == 'memory':
            await self.simulate_memory_leaks(server_url, intensity)
        elif test_type == 'race':
            await self.simulate_race_conditions(server_url, intensity)
        elif test_type == 'both':
            await self.simulate_memory_leaks(server_url, intensity)
            await asyncio.sleep(1)
            await self.simulate_race_conditions(server_url, intensity)

        # await self.analyze_results(server_url, "Target")

    async def run_gradual_memory_test(self, server_url: str):
        """Run very gradual memory test for easy monitoring"""
        print(f"🐌 Starting ultra-gradual memory test on {server_url}")
        print("💡 Perfect for running alongside: watch -n 1 'curl -s http://localhost:8000/api/v1/system/diagnostics | jq {...}'")
        print("-" * 80)

        if not await self.test_server_connectivity(server_url):
            print("❌ Server not reachable")
            return

        # Get initial baseline
        print("📊 Getting initial baseline...")
        baseline = await self.get_memory_dump(server_url)
        if baseline:
            print(f"   Initial Memory: {baseline.get('memory_usage_mb', 0):.2f} MB")
            print(f"   Initial Cache: {baseline.get('transaction_cache_size', 0)}")

        async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
            # Three powerful waves for rapid memory growth
            increments = [20, 40, 60]  # Aggressive increase in 3 waves

            for step, increment in enumerate(increments, 1):
                print(f"\n🔄 Step {step}/{len(increments)}: Adding {increment} transactions...")

                # Add transactions gradually
                for i in range(increment):
                    data = {
                        "data": f"gradual_test_{step}_{i}_{time.time()}",
                        "iterations": random.randint(1, 3)
                    }
                    await self.make_request(session, f"{server_url}/api/v1/transactions/process", data)

                    if i % 5 == 0:  # Mini-pause every 5 requests
                        await asyncio.sleep(0.5)

                # Add sessions (aggressive ratio for fewer waves)
                session_count = increment // 2  # Half as many sessions as transactions
                for i in range(session_count):
                    user_id = f"gradual_user_{step}_{i}_{int(time.time())}"
                    await self.make_request(session, f"{server_url}/api/v1/accounts/session?user_id={user_id}")

                # Add reports for all steps (more memory pressure)
                report_count = increment // 20  # 1 report per 20 transactions
                for i in range(report_count):
                    await self.make_request(session, f"{server_url}/api/v1/reports/generate")
                    await asyncio.sleep(0.5)

                # Show current stats
                current_stats = await self.get_memory_dump(server_url)
                if current_stats:
                    memory_diff = current_stats.get('memory_usage_mb', 0) - baseline.get('memory_usage_mb', 0) if baseline else 0
                    cache_diff = current_stats.get('transaction_cache_size', 0) - baseline.get('transaction_cache_size', 0) if baseline else 0
                    print(f"   📈 Memory: {current_stats.get('memory_usage_mb', 0):.2f} MB (+{memory_diff:.2f})")
                    print(f"   📦 Cache: {current_stats.get('transaction_cache_size', 0)} (+{cache_diff})")
                    print(f"   👥 Sessions: {current_stats.get('active_user_sessions', 0)}")
                    print(f"   📊 Reports: {current_stats.get('pending_reports', 0)}")

                # No pause - continuous memory growth
                print(f"   ✅ Step {step}/{len(increments)} complete")

            print("\n🎉 Gradual memory test completed!")
            print("💡 Check final memory usage with: curl http://localhost:8000/api/v1/system/diagnostics | jq")

    async def check_memory_threshold(self, server_url: str, threshold_mb: float = 200.0):
        """Check if memory usage exceeds threshold. Returns (exceeded, current_memory)."""
        try:
            memory_dump = await self.get_memory_dump(server_url)
            if memory_dump:
                current_memory = memory_dump.get('memory_usage_mb', 0)
                return current_memory > threshold_mb, current_memory
            return False, None
        except Exception:
            return False, None  # Continue test if we can't check

    async def analyze_and_generate_report(self, server_url: str, server_name: str):
        """Analyze server state and generate markdown report"""
        print(f"\n🔍 Analyzing {server_name} server and generating report...")

        # Get comprehensive data
        memory_dump = None
        thread_dump = None
        risk_assessment = None

        print("📊 Gathering system diagnostics...")
        for attempt in range(2):  # Fewer retries for speed
            memory_dump = await self.get_memory_dump(server_url)
            if memory_dump:
                break
            await asyncio.sleep(1)

        print("🧵 Gathering concurrency metrics...")
        for attempt in range(2):
            thread_dump = await self.get_thread_dump(server_url)
            if thread_dump:
                break
            await asyncio.sleep(1)

        print("📈 Gathering risk assessment...")
        try:
            async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
                risk_assessment = await self.make_request(session, f"{server_url}/api/v1/analytics/risk-assessment", method="GET", timeout=30)
        except Exception as e:
            print(f"Failed to get risk assessment: {e}")

        # Generate markdown report
        self.generate_markdown_report(memory_dump, thread_dump, risk_assessment, server_name)

        # Also show console summary
        self.show_console_summary(memory_dump, thread_dump)

    def show_console_summary(self, memory_dump, thread_dump):
        """Show quick console summary"""
        print(f"\n📋 Load Test Summary:")
        if memory_dump:
            print(f"   📈 Memory Usage: {memory_dump.get('memory_usage_mb', 0):.2f} MB")
            print(f"   📦 Cache Size: {memory_dump.get('transaction_cache_size', 0)}")
            print(f"   👥 Sessions: {memory_dump.get('active_user_sessions', 0)}")
            print(f"   📊 Reports: {memory_dump.get('pending_reports', 0)}")

            # Memory leak indicators
            if memory_dump.get('memory_usage_mb', 0) > 100:
                print("   🚨 HIGH MEMORY USAGE - Memory leak detected!")
            if memory_dump.get('transaction_cache_size', 0) > 30:
                print("   🚨 LARGE CACHE - Cache leak detected!")

        if thread_dump:
            banking_state = thread_dump.get('banking_system_state', {})
            total_deposits = banking_state.get('total_deposits', 0)
            print(f"   💰 Bank Deposits: {total_deposits}")
            if total_deposits < 0:
                print("   🚨 NEGATIVE DEPOSITS - Race condition detected!")

    def generate_markdown_report(self, memory_dump, thread_dump, risk_assessment, server_name):
        """Generate detailed markdown report"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"load_test_report_{timestamp}.md"

        report_content = f"""# SecureBank Load Test Report

## Test Overview
- **Server**: {server_name}
- **Timestamp**: {datetime.now().isoformat()}
- **Test Type**: Comprehensive Load Test (Memory Leaks + Race Conditions)

## Memory Analysis
"""

        if memory_dump:
            report_content += f"""
### System Memory
- **Memory Usage**: {memory_dump.get('memory_usage_mb', 0):.2f} MB
- **Virtual Memory**: {memory_dump.get('virtual_memory_mb', 0):.2f} MB
- **System Status**: {memory_dump.get('system_status', 'unknown')}

### Memory Leaks Detected
- **Transaction Cache**: {memory_dump.get('transaction_cache_size', 0)} items
- **Active Sessions**: {memory_dump.get('active_user_sessions', 0)} sessions
- **Pending Reports**: {memory_dump.get('pending_reports', 0)} reports
- **Validation Cache**: {memory_dump.get('validation_cache_size', 0)} items
- **Customer Profiles**: {memory_dump.get('customer_profiles', 0)} profiles

### Memory Leak Severity
"""

            # Add severity analysis
            cache_size = memory_dump.get('transaction_cache_size', 0)
            memory_mb = memory_dump.get('memory_usage_mb', 0)
            sessions = memory_dump.get('active_user_sessions', 0)

            if memory_mb > 200:
                report_content += "- 🔴 **CRITICAL**: Memory usage > 200MB\n"
            elif memory_mb > 100:
                report_content += "- 🟡 **WARNING**: Memory usage > 100MB\n"
            else:
                report_content += "- 🟢 **NORMAL**: Memory usage acceptable\n"

            if cache_size > 50:
                report_content += "- 🔴 **CRITICAL**: Transaction cache > 50 items\n"
            elif cache_size > 25:
                report_content += "- 🟡 **WARNING**: Transaction cache > 25 items\n"
            else:
                report_content += "- 🟢 **NORMAL**: Transaction cache size acceptable\n"

        else:
            report_content += "\n❌ **Memory diagnostics unavailable** - Server may be overloaded\n"

        report_content += "\n## Concurrency Analysis\n"

        if thread_dump:
            banking_state = thread_dump.get('banking_system_state', {})
            report_content += f"""
### Banking System State
- **Total Deposits**: ${banking_state.get('total_deposits', 0):,.2f}
- **Portfolio Positions**: {banking_state.get('portfolio_positions', 0)}
- **Active Traders**: {banking_state.get('active_traders', 0)}
- **Audit Entries**: {banking_state.get('audit_entries', 0)}

### Threading Metrics
- **Concurrent Workers**: {thread_dump.get('concurrent_workers', 0)}
- **Report Processors**: {len(thread_dump.get('report_processors', {}))}

### Race Condition Detection
"""

            total_deposits = banking_state.get('total_deposits', 0)
            if total_deposits < 0:
                report_content += "- 🔴 **RACE CONDITION DETECTED**: Negative bank deposits detected\n"
            elif total_deposits < 500000:  # Started with 1M
                report_content += "- 🟡 **POTENTIAL ISSUE**: Significant deposit reduction\n"
            else:
                report_content += "- 🟢 **NORMAL**: Bank deposits within expected range\n"

        else:
            report_content += "\n❌ **Concurrency metrics unavailable** - Server may be overloaded\n"

        report_content += "\n## Risk Assessment\n"

        if risk_assessment:
            risk_data = risk_assessment.get('risk_analysis', {})
            report_content += f"""
### Memory Risk Analysis
- **Transaction Cache**: {risk_data.get('transaction_cache_size_mb', 0):.2f} MB
- **Active Sessions**: {risk_data.get('active_sessions_mb', 0):.2f} MB
- **Report Queue**: {risk_data.get('report_queue_mb', 0):.2f} MB
- **Linked Accounts**: {risk_data.get('linked_accounts', 0)}

### Recommendations
"""
            recommendations = risk_assessment.get('compliance_recommendations', [])
            for rec in recommendations:
                report_content += f"- {rec}\n"
        else:
            report_content += "\n❌ **Risk assessment unavailable**\n"

        report_content += f"""

## Test Configuration
- **Memory Test**: Transaction cache, user sessions, background reports
- **Race Condition Test**: Concurrent transfers, portfolio updates
- **Intensity**: Heavy (optimized for 30-second execution)

## Summary
"""

        # Add overall summary
        issues_found = []
        if memory_dump:
            if memory_dump.get('memory_usage_mb', 0) > 100:
                issues_found.append("Memory leak detected")
            if memory_dump.get('transaction_cache_size', 0) > 25:
                issues_found.append("Cache leak detected")

        if thread_dump:
            banking_state = thread_dump.get('banking_system_state', {})
            if banking_state.get('total_deposits', 0) < 0:
                issues_found.append("Race condition detected")

        if issues_found:
            report_content += f"**Issues Detected**: {', '.join(issues_found)}\n"
            report_content += "\n🚨 **The load test successfully demonstrated system vulnerabilities.**\n"
        else:
            report_content += "\n✅ **No critical issues detected during this test run.**\n"

        # Check if test was stopped early due to memory
        if memory_dump and memory_dump.get('memory_usage_mb', 0) > 350:
            report_content += "\n⚠️  **Note**: Test may have been stopped early due to high memory usage for system safety.\n"

        report_content += f"""

## Commands for Further Investigation
```bash
# Get current memory status
curl http://localhost:8000/api/v1/system/diagnostics | jq

# Monitor memory in real-time
watch -n 2 'curl -s http://localhost:8000/api/v1/system/diagnostics | jq ".memory_usage_mb, .transaction_cache_size"'

# Check concurrency metrics
curl http://localhost:8000/api/v1/system/concurrency | jq

# Clean up system (if needed)
curl -X POST http://localhost:8000/api/v1/admin/system-maintenance
```

---
*Report generated by SecureBank Load Tester*
"""

        # Write report to file
        try:
            with open(filename, 'w') as f:
                f.write(report_content)
            print(f"\n📝 **Report Generated**: {filename}")
        except Exception as e:
            print(f"\n❌ Failed to write report: {e}")

def main():
    """Main entry point with command line arguments"""

    if len(sys.argv) == 1:
        # Default: run memory test with heavy intensity
        tester = SecureBankLoadTester()
        asyncio.run(tester.run_single_test(tester.server_url, 'memory', 'heavy'))
        return

    command = sys.argv[1].lower()

    if command in ['help', '--help', '-h']:
        print("SecureBank Load Tester")
        print("Usage:")
        print("  python load_test.py                    # Run comprehensive test")
        print("  python load_test.py server [intensity] # Test SecureBank server only")
        print("  python load_test.py memory [url]       # Memory leak test only (gradual)")
        print("  python load_test.py race [url]         # Race condition test only")
        print("  python load_test.py gradual [url]      # Slow gradual memory test")
        print("  python load_test.py analyze [url]      # Analyze server state")
        print("")
        print("Intensities: light, medium, heavy, extreme")
        print("Default intensity: heavy")
        return

    tester = SecureBankLoadTester()

    if command == 'server':
        intensity = sys.argv[2] if len(sys.argv) > 2 else 'heavy'
        target_servers = [(tester.server_url, "SecureBank")]
        asyncio.run(tester.run_comprehensive_test(target_servers, intensity))

    # elif command == 'fixed':
    #     intensity = sys.argv[2] if len(sys.argv) > 2 else 'medium'
    #     target_servers = [(tester.fixed_url, "Fixed")]
    #     asyncio.run(tester.run_comprehensive_test(target_servers, intensity))

    elif command == 'memory':
        url = sys.argv[2] if len(sys.argv) > 2 else tester.server_url
        intensity = sys.argv[3] if len(sys.argv) > 3 else 'heavy'
        asyncio.run(tester.run_single_test(url, 'memory', intensity))

    elif command == 'race':
        url = sys.argv[2] if len(sys.argv) > 2 else tester.server_url
        intensity = sys.argv[3] if len(sys.argv) > 3 else 'heavy'
        asyncio.run(tester.run_single_test(url, 'race', intensity))

    elif command == 'gradual':
        url = sys.argv[2] if len(sys.argv) > 2 else tester.server_url
        print("🐌 Running ultra-gradual memory test for easy monitoring...")
        asyncio.run(tester.run_gradual_memory_test(url))

    elif command == 'analyze':
        url = sys.argv[2] if len(sys.argv) > 2 else tester.server_url
        asyncio.run(tester.analyze_results(url, "Target"))

    else:
        print(f"Unknown command: {command}")
        print("Use 'python load_test.py help' for usage information")

if __name__ == "__main__":
    main()
