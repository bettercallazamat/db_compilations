import json
import argparse
from datetime import datetime
from pymongo import MongoClient
from compilation_manager import CompilationManager
from compilation_parser import CompilationParser, VideoUsageTracker
from db_schema import initialize_database


class VideoAnalyticsUtils:
    """Utility functions for video analytics and compilation management"""

    def __init__(self, mongo_uri="mongodb://localhost:27017/video_database"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.video_database
        self.videos_collection = self.db.videos
        self.compilations_collection = self.db.compilations
        self.compilation_manager = CompilationManager(
            self.videos_collection,
            self.compilations_collection
        )

    def analyze_compilation_keywords(self):
        """Analyze all video descriptions to find compilation keywords"""
        print("🔍 Analyzing compilation keywords in video descriptions...")

        print("\n📹 Processing all compilations...")
        processing_results = self.compilation_manager.process_all_compilations()

        print(f"   ✅ Processing completed:")
        print(f"     - Videos processed: {processing_results['processed']}")
        print(f"     - New compilations: {processing_results['new_compilations']}")
        print(
            f"     - Updated compilations: {processing_results['updated_compilations']}")
        if processing_results['errors']:
            print(
                f"     - Errors encountered: {len(processing_results['errors'])}")
            for error in processing_results['errors'][:3]:  # Show first 3 errors
                print(f"       • {error}")

        compilation_keywords = {}
        potential_compilations = []

        videos = self.videos_collection.find({})
        total_videos = self.videos_collection.count_documents({})

        for i, video in enumerate(videos):
            if i % 100 == 0:
                print(f"   Processed {i}/{total_videos} videos...")

            description = video.get('description', '').lower()

            # Check for existing keywords
            for keyword in CompilationParser.COMPILATION_KEYWORDS:
                if keyword.lower() in description:
                    if keyword not in compilation_keywords:
                        compilation_keywords[keyword] = []
                    compilation_keywords[keyword].append({
                        'video_id': video['video_id'],
                        'title': video['title']
                    })

            # Look for other potential compilation indicators
            potential_indicators = [
                # 'compilation', 'mega', '+ more', 'collection',
                # 'best of', 'minutes of', 'hours of', 'non-stop'
            ]

            for indicator in potential_indicators:
                if indicator in description and not CompilationParser.is_compilation(description):
                    potential_compilations.append({
                        'video_id': video['video_id'],
                        'title': video['title'],
                        'indicator': indicator
                    })

        print(f"\n📊 Compilation Keyword Analysis Results:")
        for keyword, videos in compilation_keywords.items():
            print(f"   '{keyword}': {len(videos)} videos")

        print(
            f"\n🤔 Potential additional compilations: {len(potential_compilations)}")
        if potential_compilations:
            print("   Top 10 potential compilations:")
            for comp in potential_compilations[:10]:
                print(
                    f"     - {comp['title']} (indicator: {comp['indicator']})")

        return compilation_keywords, potential_compilations

    def generate_usage_report(self, output_file='video_usage_report.json'):
        """Generate comprehensive usage report for all videos"""
        print("📈 Generating comprehensive video usage report...")

        # Update usage statistics first
        self.compilation_manager.update_video_usage_statistics()

        # Get all videos with usage stats
        videos_with_stats = list(self.videos_collection.find({
            'compilation_usage_stats': {'$exists': True}
        }))

        report = {
            'generated_at': datetime.utcnow().isoformat(),
            'total_videos_tracked': len(videos_with_stats),
            'summary': {
                'most_used_videos': [],
                'most_used_as_first': [],
                'usage_by_duration': {},
                'total_inclusions': 0
            },
            'detailed_stats': []
        }

        total_inclusions = 0
        duration_usage = {}

        # Process each video
        for video in videos_with_stats:
            stats = video.get('compilation_usage_stats', {})
            total_video_inclusions = stats.get('total_inclusions', 0)
            total_inclusions += total_video_inclusions

            # Track usage by duration
            for duration, count in stats.get('usage_by_duration', {}).items():
                if duration not in duration_usage:
                    duration_usage[duration] = 0
                duration_usage[duration] += count

            if total_video_inclusions > 0:
                video_report = {
                    'video_id': video['video_id'],
                    'title': video['title'],
                    'total_inclusions': total_video_inclusions,
                    'first_video_count': stats.get('first_video_count', 0),
                    'usage_by_duration': stats.get('usage_by_duration', {}),
                    'first_video_by_duration': stats.get('first_video_by_duration', {}),
                    'view_count': video.get('view_count', 0),
                    'duration_seconds': video.get('duration_seconds', 0)
                }
                report['detailed_stats'].append(video_report)

        # Sort and get top videos
        report['detailed_stats'].sort(
            key=lambda x: x['total_inclusions'], reverse=True)
        report['summary']['most_used_videos'] = report['detailed_stats'][:20]
        report['summary']['most_used_as_first'] = sorted(
            report['detailed_stats'],
            key=lambda x: x['first_video_count'],
            reverse=True
        )[:20]

        report['summary']['total_inclusions'] = total_inclusions
        report['summary']['usage_by_duration'] = duration_usage

        # Save report to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(f"✅ Usage report saved to {output_file}")
        print(f"   Total videos tracked: {len(videos_with_stats)}")
        print(f"   Total inclusions: {total_inclusions}")
        print(
            f"   Most used video: {report['summary']['most_used_videos'][0]['title'] if report['summary']['most_used_videos'] else 'N/A'}")

        return report

    def find_duplicate_compilations(self):
        """Find potential duplicate compilations"""
        print("🔍 Looking for duplicate compilations...")

        compilations = list(self.compilations_collection.find({}))
        duplicates = []

        for i, comp1 in enumerate(compilations):
            for j, comp2 in enumerate(compilations[i+1:], i+1):
                # Check if titles are very similar
                title1 = comp1['title'].lower().strip()
                title2 = comp2['title'].lower().strip()

                # Simple similarity check
                if (title1 in title2 or title2 in title1) and title1 != title2:
                    duplicates.append({
                        'compilation1': {
                            'id': str(comp1['_id']),
                            'title': comp1['title'],
                            'duration': comp1['duration_rounded']
                        },
                        'compilation2': {
                            'id': str(comp2['_id']),
                            'title': comp2['title'],
                            'duration': comp2['duration_rounded']
                        }
                    })

        print(f"   Found {len(duplicates)} potential duplicate pairs")
        for dup in duplicates[:10]:  # Show first 10
            print(
                f"     - '{dup['compilation1']['title']}' vs '{dup['compilation2']['title']}'")

        return duplicates

    def export_compilations_data(self, output_file='compilations_export.json'):
        """Export all compilations data for backup or analysis"""
        print(f"📤 Exporting compilations data to {output_file}...")

        compilations = list(self.compilations_collection.find({}))

        # Convert ObjectId to string for JSON serialization
        for comp in compilations:
            comp['_id'] = str(comp['_id'])
            if 'original_video_id' in comp:
                comp['original_video_id'] = str(comp['original_video_id'])

        export_data = {
            'exported_at': datetime.utcnow().isoformat(),
            'total_compilations': len(compilations),
            'compilations': compilations,
            'statistics': self.compilation_manager.get_compilation_statistics()
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Exported {len(compilations)} compilations")
        return export_data

    def validate_compilation_timestamps(self):
        """Validate that compilation timestamps are properly formatted"""
        print("✅ Validating compilation timestamps...")

        issues = []
        valid_count = 0
        total_count = 0

        compilations = self.compilations_collection.find({})

        for compilation in compilations:
            total_count += 1
            timestamps = compilation.get('timestamps', [])
            compilation_issues = []

            for i, timestamp_entry in enumerate(timestamps):
                timestamp = timestamp_entry.get('timestamp', '')
                title = timestamp_entry.get('title', '')

                # Validate timestamp format
                if not timestamp or not title:
                    compilation_issues.append(
                        f"Empty timestamp or title at index {i}")
                    continue

                # Check timestamp format (MM:SS or H:MM:SS)
                if not CompilationParser.timestamp_to_seconds(timestamp):
                    compilation_issues.append(
                        f"Invalid timestamp format: {timestamp}")

                # Check if titles are too short or too long
                if len(title.strip()) < 3:
                    compilation_issues.append(f"Title too short: '{title}'")
                elif len(title) > 200:
                    compilation_issues.append(
                        f"Title too long: '{title[:50]}...'")

            if compilation_issues:
                issues.append({
                    'compilation_id': str(compilation['_id']),
                    'title': compilation['title'],
                    'issues': compilation_issues
                })
            else:
                valid_count += 1

        print(f"   Validated {total_count} compilations")
        print(f"   ✅ Valid: {valid_count}")
        print(f"   ❌ With issues: {len(issues)}")

        if issues:
            print("   Top issues:")
            for issue in issues[:5]:
                print(
                    f"     - {issue['title']}: {len(issue['issues'])} issues")

        return issues


def main():
    """Command-line interface for utility functions"""
    parser = argparse.ArgumentParser(description='Video Analytics Utilities')
    parser.add_argument('command', choices=[
        'init', 'analyze', 'report', 'duplicates', 'export', 'validate'
    ], help='Command to execute')
    parser.add_argument('--output', '-o', help='Output file path')

    args = parser.parse_args()

    if args.command == 'init':
        initialize_database()
    else:
        utils = VideoAnalyticsUtils()

        if args.command == 'analyze':
            utils.analyze_compilation_keywords()
        elif args.command == 'report':
            output_file = args.output or 'video_usage_report.json'
            utils.generate_usage_report(output_file)
        elif args.command == 'duplicates':
            utils.find_duplicate_compilations()
        elif args.command == 'export':
            output_file = args.output or 'compilations_export.json'
            utils.export_compilations_data(output_file)
        elif args.command == 'validate':
            utils.validate_compilation_timestamps()


if __name__ == '__main__':
    main()
