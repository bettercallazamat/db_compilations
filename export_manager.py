from datetime import datetime, timedelta
from typing import Dict, List, Optional
from bson.objectid import ObjectId
import os
import json


class CompilationExporter:
    """
    Comprehensive export system for user-created compilations.
    Supports multiple export formats with detailed metadata and analytics.
    """

    def __init__(self, user_compilations_collection, videos_collection):
        self.user_compilations_collection = user_compilations_collection
        self.videos_collection = videos_collection

        # Export configuration
        self.export_directory = "exports"
        self.ensure_export_directory()

    def ensure_export_directory(self):
        """Create export directory if it doesn't exist"""
        if not os.path.exists(self.export_directory):
            os.makedirs(self.export_directory)

    def export_compilation_to_txt(self, compilation_id: str, include_analytics: bool = True,
                                  include_metadata: bool = True) -> Dict:
        """
        Export a compilation to a comprehensive text file format suitable for
        video production teams, content creators, and analytical review.

        Args:
            compilation_id: ID of the compilation to export
            include_analytics: Whether to include detailed analytics in the export
            include_metadata: Whether to include creation metadata

        Returns:
            Dictionary with export results and file information
        """
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return {
                    'success': False,
                    'error': 'Compilation not found',
                    'file_path': None
                }

            # Generate comprehensive text content
            content = self._generate_txt_content(
                compilation, include_analytics, include_metadata)

            # Create filename with timestamp and sanitized title
            safe_title = self._sanitize_filename(
                compilation.get('title', 'Untitled'))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_title}_{timestamp}.txt"
            file_path = os.path.join(self.export_directory, filename)

            # Write content to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            # Update export statistics in database
            self._update_export_stats(compilation_id)

            # Get file size for return info
            file_size = os.path.getsize(file_path)

            return {
                'success': True,
                'file_path': file_path,
                'filename': filename,
                'file_size_bytes': file_size,
                'export_timestamp': datetime.now().isoformat(),
                'compilation_title': compilation.get('title', 'Untitled'),
                'video_count': compilation.get('video_count', 0)
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Export failed: {str(e)}',
                'file_path': None
            }

    def _generate_txt_content(self, compilation: Dict, include_analytics: bool,
                              include_metadata: bool) -> str:
        """
        Generate simplified text content with compilation name and video list.
        """
        content_lines = []

        # First line: compilation name
        compilation_title = compilation.get('title', 'Untitled Compilation')
        content_lines.append(compilation_title)
        content_lines.append("")  # Empty line after title

        # Process each video in the timeline
        timestamps = compilation.get('timestamps', [])
        for timestamp_entry in timestamps:
            # Format timestamp and title
            timestamp = timestamp_entry.get('timestamp', '0:00')
            title = timestamp_entry.get('title', 'Untitled Video')
            
            # Clean the title by removing everything after "|" character
            clean_title = self._clean_video_title(title)

            # Add timestamp and video name
            content_lines.append(f"{timestamp} {clean_title}")

        return "\n".join(content_lines)

    def _format_duration_seconds(self, seconds: int) -> str:
        """Format seconds into human-readable duration (MM:SS or H:MM:SS)"""
        if seconds <= 0:
            return "0:00"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60

        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"

    def _format_number(self, number: int) -> str:
        """Format large numbers with appropriate suffixes"""
        if number >= 1_000_000:
            return f"{number / 1_000_000:.1f}M"
        elif number >= 1_000:
            return f"{number / 1_000:.1f}K"
        else:
            return str(number)

    def _clean_video_title(self, title: str) -> str:
        """Clean video title by removing everything after the '|' character"""
        if '|' in title:
            return title.split('|')[0].strip()
        return title.strip()

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe file system usage"""
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')

        # Limit length and remove extra spaces
        filename = filename.strip()[:50]  # Limit to 50 characters
        # Replace spaces with underscores
        filename = '_'.join(filename.split())

        return filename or "compilation"

    def _update_export_stats(self, compilation_id: str):
        """Update export statistics in the database"""
        try:
            self.user_compilations_collection.update_one(
                {'_id': ObjectId(compilation_id)},
                {
                    '$set': {
                        'export_data.last_exported': datetime.now()
                    },
                    '$inc': {
                        'export_data.export_count': 1
                    }
                }
            )
        except Exception:
            pass  # Non-critical operation, don't fail export if this fails

    def _generate_quality_breakdown(self, timestamps: List[Dict]) -> str:
        """Generate a breakdown of video quality distribution"""
        if not timestamps:
            return "No videos to analyze"

        retention_rates = []
        for timestamp_entry in timestamps:
            retention = timestamp_entry.get('retention_rate', 0)
            retention_rates.append(retention)

        if not retention_rates:
            return "Retention data not available"

        avg_retention = sum(retention_rates) / len(retention_rates)
        high_quality = len([r for r in retention_rates if r >= 70])
        medium_quality = len([r for r in retention_rates if 50 <= r < 70])
        low_quality = len([r for r in retention_rates if r < 50])

        return f"""
• Average Retention Rate: {avg_retention:.1f}%
• High Quality Videos (70%+ retention): {high_quality}
• Medium Quality Videos (50-69% retention): {medium_quality}
• Lower Quality Videos (<50% retention): {low_quality}"""

    def export_multiple_compilations(self, compilation_ids: List[str],
                                     include_analytics: bool = True) -> Dict:
        """
        Export multiple compilations to individual text files with batch processing
        and comprehensive reporting of the export operation.

        Args:
            compilation_ids: List of compilation IDs to export
            include_analytics: Whether to include analytics in exports

        Returns:
            Dictionary with batch export results
        """
        export_results = []
        successful_exports = 0
        failed_exports = 0

        for compilation_id in compilation_ids:
            result = self.export_compilation_to_txt(
                compilation_id,
                include_analytics=include_analytics,
                include_metadata=True
            )

            if result['success']:
                successful_exports += 1
            else:
                failed_exports += 1

            export_results.append({
                'compilation_id': compilation_id,
                'result': result
            })

        # Create batch summary file
        batch_summary = self._create_batch_summary(export_results)

        return {
            'success': failed_exports == 0,
            'total_processed': len(compilation_ids),
            'successful_exports': successful_exports,
            'failed_exports': failed_exports,
            'individual_results': export_results,
            'batch_summary_file': batch_summary,
            'export_directory': self.export_directory
        }

    def _create_batch_summary(self, export_results: List[Dict]) -> str:
        """Create a summary file for batch exports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_filename = f"batch_export_summary_{timestamp}.txt"
        summary_path = os.path.join(self.export_directory, summary_filename)

        summary_lines = [
            "=" * 60,
            "BATCH EXPORT SUMMARY",
            "=" * 60,
            "",
            f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Compilations Processed: {len(export_results)}",
            "",
            "EXPORT RESULTS:",
            "-" * 40
        ]

        for i, result in enumerate(export_results, 1):
            comp_result = result['result']
            status = "✓ SUCCESS" if comp_result['success'] else "✗ FAILED"

            summary_lines.extend([
                f"{i}. {status}",
                f"   Compilation ID: {result['compilation_id']}",
                f"   Title: {comp_result.get('compilation_title', 'Unknown')}"
            ])

            if comp_result['success']:
                summary_lines.extend([
                    f"   File: {comp_result['filename']}",
                    f"   Videos: {comp_result['video_count']}",
                    f"   Size: {comp_result['file_size_bytes']} bytes"
                ])
            else:
                summary_lines.append(f"   Error: {comp_result['error']}")

            summary_lines.append("")

        # Write summary file
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(summary_lines))

        return summary_path

    def export_compilation_to_json(self, compilation_id: str) -> Dict:
        """
        Export compilation data in JSON format for API integration,
        data analysis, or backup purposes.

        Args:
            compilation_id: ID of the compilation to export

        Returns:
            Dictionary with export results and file information
        """
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return {
                    'success': False,
                    'error': 'Compilation not found',
                    'file_path': None
                }

            # Prepare data for JSON export (handle ObjectId serialization)
            export_data = self._prepare_json_export_data(compilation)

            # Create filename
            safe_title = self._sanitize_filename(
                compilation.get('title', 'Untitled'))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_title}_{timestamp}.json"
            file_path = os.path.join(self.export_directory, filename)

            # Write JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2,
                          ensure_ascii=False, default=str)

            # Update export statistics
            self._update_export_stats(compilation_id)

            return {
                'success': True,
                'file_path': file_path,
                'filename': filename,
                'file_size_bytes': os.path.getsize(file_path),
                'export_timestamp': datetime.now().isoformat(),
                'format': 'JSON'
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'JSON export failed: {str(e)}',
                'file_path': None
            }

    def _prepare_json_export_data(self, compilation: Dict) -> Dict:
        """Prepare compilation data for JSON export with enhanced video details"""
        # Convert ObjectId to string
        export_data = compilation.copy()
        export_data['_id'] = str(compilation['_id'])

        # Enhance timestamp entries with full video details
        enhanced_timestamps = []
        for timestamp_entry in compilation.get('timestamps', []):
            video_id = timestamp_entry.get('video_id')
            video_details = self.videos_collection.find_one(
                {'video_id': video_id})

            enhanced_entry = timestamp_entry.copy()
            if video_details:
                # Add comprehensive video information
                enhanced_entry['video_details'] = {
                    'title': video_details.get('title', ''),
                    'description': video_details.get('description', ''),
                    'duration_seconds': video_details.get('duration_seconds', 0),
                    'view_count': video_details.get('view_count', 0),
                    'like_count': video_details.get('like_count', 0),
                    'comment_count': video_details.get('comment_count', 0),
                    'published_at': video_details.get('published_at', ''),
                    'thumbnail_url': video_details.get('thumbnail_url', ''),
                    'average_view_percentage': video_details.get('average_view_percentage', 0),
                    'retention_30s': video_details.get('retention_30s', 0),
                    'compilation_usage_stats': video_details.get('compilation_usage_stats', {})
                }

            enhanced_timestamps.append(enhanced_entry)

        export_data['enhanced_timestamps'] = enhanced_timestamps
        export_data['export_metadata'] = {
            'export_timestamp': datetime.now().isoformat(),
            'export_format': 'JSON',
            'export_version': '1.0',
            'total_videos': len(enhanced_timestamps)
        }

        return export_data

    def get_export_history(self, compilation_id: Optional[str] = None) -> List[Dict]:
        """
        Get export history for compilations with detailed statistics
        and file information.

        Args:
            compilation_id: Optional specific compilation ID to get history for

        Returns:
            List of export history records
        """
        query = {}
        if compilation_id:
            query['_id'] = ObjectId(compilation_id)

        # Get compilations with export data
        compilations = list(self.user_compilations_collection.find(
            query,
            {
                'title': 1,
                'created_at': 1,
                'status': 1,
                'export_data': 1,
                'video_count': 1,
                'duration_rounded': 1
            }
        ))

        export_history = []
        for comp in compilations:
            export_data = comp.get('export_data', {})

            history_entry = {
                'compilation_id': str(comp['_id']),
                'title': comp.get('title', 'Untitled'),
                'status': comp.get('status', 'unknown'),
                'video_count': comp.get('video_count', 0),
                'duration_minutes': comp.get('duration_rounded', 0),
                'created_at': comp.get('created_at'),
                'export_count': export_data.get('export_count', 0),
                'last_exported': export_data.get('last_exported'),
                'never_exported': export_data.get('export_count', 0) == 0
            }

            export_history.append(history_entry)

        # Sort by last exported date (most recent first)
        export_history.sort(
            key=lambda x: x['last_exported'] if x['last_exported'] else datetime.min,
            reverse=True
        )

        return export_history

    def cleanup_old_exports(self, days_old: int = 30) -> Dict:
        """
        Clean up old export files to manage disk space usage.

        Args:
            days_old: Number of days after which files should be considered old

        Returns:
            Dictionary with cleanup results
        """
        if not os.path.exists(self.export_directory):
            return {
                'success': True,
                'files_deleted': 0,
                'space_freed_bytes': 0,
                'message': 'Export directory does not exist'
            }

        cutoff_date = datetime.now() - timedelta(days=days_old)
        files_deleted = 0
        space_freed = 0

        try:
            for filename in os.listdir(self.export_directory):
                file_path = os.path.join(self.export_directory, filename)

                if os.path.isfile(file_path):
                    file_modified = datetime.fromtimestamp(
                        os.path.getmtime(file_path))

                    if file_modified < cutoff_date:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        files_deleted += 1
                        space_freed += file_size

            return {
                'success': True,
                'files_deleted': files_deleted,
                'space_freed_bytes': space_freed,
                'space_freed_mb': round(space_freed / (1024 * 1024), 2),
                'cutoff_date': cutoff_date.isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Cleanup failed: {str(e)}',
                'files_deleted': files_deleted,
                'space_freed_bytes': space_freed
            }
