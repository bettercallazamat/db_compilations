from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from bson.objectid import ObjectId
import math
from enum import Enum


class CompilationStatus(Enum):
    """Enumeration for compilation publication status"""
    DRAFT = "draft"
    NOT_PUBLISHED = "not_published"
    PUBLISHED = "published"


class VideoCategory(Enum):
    """Video categories based on retention rate percentiles"""
    TOP_25_PERCENT = "top_25"
    SECOND_25_PERCENT = "second_25"
    THIRD_25_PERCENT = "third_25"
    BOTTOM_25_PERCENT = "bottom_25"


class CompilationCreator:
    """
    Advanced compilation creation system that intelligently selects videos
    based on multiple criteria including retention rates, usage frequency,
    publication dates, and duration constraints.
    """

    def __init__(self, videos_collection, compilations_collection, user_compilations_collection):
        self.videos_collection = videos_collection
        self.compilations_collection = compilations_collection
        self.user_compilations_collection = user_compilations_collection

        # Configuration constants for compilation creation logic
        # Maximum times a video can be used in compilations per year
        self.MAX_ANNUAL_USAGE = 10
        # Maximum duration for individual videos in compilations (in seconds)
        self.MAX_VIDEO_DURATION_SECONDS = 300  # 5 minutes
        self.RETENTION_WEIGHT = 0.6  # Weight factor for retention rate in scoring algorithm
        self.VIEW_COUNT_WEIGHT = 0.1  # Weight factor for view count in scoring algorithm
        self.FRESHNESS_WEIGHT = 0.1  # Weight factor for video freshness in scoring algorithm

    def get_available_durations(self) -> List[int]:
        """
        Retrieve all available compilation durations based on existing compilations
        in the database, rounded to nearest 5-minute intervals.
        
        Returns:
            List[int]: Sorted list of available duration options (5, 10, 15, 20, etc.)
        """
        pipeline = [
            {'$match': {'duration_rounded': {'$exists': True}}},
            {'$group': {'_id': '$duration_rounded'}},
            {'$sort': {'_id': 1}}
        ]

        existing_durations = list(
            self.compilations_collection.aggregate(pipeline))
        durations = [dur['_id'] for dur in existing_durations]

        # Add common duration options if not present
        standard_durations = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
        for duration in standard_durations:
            if duration not in durations:
                durations.append(duration)

        return sorted(durations)

    def categorize_videos_by_retention(self, videos: List[Dict], from_date: Optional[str] = None) -> Dict[VideoCategory, List[Dict]]:
        """
        Categorize videos into retention rate percentiles with sophisticated filtering
        and sorting algorithms to ensure optimal video selection.
        
        Args:
            videos: List of video documents from database
            from_date: Optional date filter in 'YYYY-MM-DD' format
            
        Returns:
            Dict mapping video categories to sorted lists of videos
        """
        # Filter videos based on criteria
        filtered_videos = []
        current_date = datetime.utcnow()
        one_year_ago = current_date - timedelta(days=365)

        for video in videos:
            # Skip compilation videos - multiple checks for robustness
            if video.get('is_compilation', False):
                continue

            # Additional compilation detection methods
            video_title = video.get('title', '').lower()
            if any(keyword in video_title for keyword in ['compilation', 'best of', 'highlights', 'collection', "more", 'songs']):
                continue

            # Check if video_id exists in compilations collection (extra safety)
            if self.compilations_collection.find_one({'video_ids': video.get('video_id')}):
                continue

            # Skip videos longer than 10 minutes
            video_duration = video.get('duration_seconds', 0)
            if video_duration > self.MAX_VIDEO_DURATION_SECONDS or video_duration ==0:
                continue

            # Apply date filter if specified
            if from_date:
                try:
                    video_date = datetime.strptime(
                        video.get('published_at', ''), '%Y-%m-%d')
                    filter_date = datetime.strptime(from_date, '%Y-%m-%d')
                    if video_date < filter_date:
                        continue
                except (ValueError, TypeError):
                    # Skip videos with invalid dates
                    continue

            # Check annual usage limit
            usage_stats = video.get('compilation_usage_stats', {})
            total_usage = usage_stats.get('total_inclusions', 0)
            if total_usage >= self.MAX_ANNUAL_USAGE:
                continue

            # Ensure video has required metrics
            if not all(key in video for key in ['retention_30s', 'view_count']):
                continue

            filtered_videos.append(video)

        if not filtered_videos:
            return {category: [] for category in VideoCategory}

        # Sort videos by comprehensive scoring algorithm
        def calculate_video_score(video: Dict) -> float:
            """
            Calculate composite score for video selection using multiple weighted factors
            """
            retention_rate = video.get('retention_30s', 0) / 100.0

            view_count = video.get('view_count', 0)

            # Normalize view count (log scale to prevent outliers from dominating)
            max_views = max(v.get('view_count', 1) for v in filtered_videos)
            normalized_views = math.log(
                view_count + 1) / math.log(max_views + 1)

            # Calculate freshness factor (newer videos get slight boost)
            try:
                video_date = datetime.strptime(
                    video.get('published_at', ''), '%Y-%m-%d')
                days_old = (current_date - video_date).days
                # Decreases linearly over a year
                freshness_factor = max(0, 1 - (days_old / 365))
            except (ValueError, TypeError):
                freshness_factor = 0

            # Composite score calculation
            score = (
                self.RETENTION_WEIGHT * retention_rate +
                self.VIEW_COUNT_WEIGHT * normalized_views +
                self.FRESHNESS_WEIGHT * freshness_factor
            )

            return score

        # Sort videos by calculated scores
        sorted_videos = sorted(
            filtered_videos, key=calculate_video_score, reverse=True)

        # Divide into quartiles (25% each)
        total_count = len(sorted_videos)
        quartile_size = total_count // 4

        categories = {
            VideoCategory.TOP_25_PERCENT: sorted_videos[:quartile_size],
            VideoCategory.SECOND_25_PERCENT: sorted_videos[quartile_size:2*quartile_size],
            VideoCategory.THIRD_25_PERCENT: sorted_videos[2*quartile_size:3*quartile_size],
            VideoCategory.BOTTOM_25_PERCENT: sorted_videos[3*quartile_size:]
        }

        return categories

    def select_first_video(self, duration_rounded: int, categorized_videos: Dict[VideoCategory, List[Dict]]) -> Optional[Dict]:
        """
        Select the optimal first video for a compilation using advanced selection criteria.
        The first video is crucial as it determines viewer engagement and retention.
        
        Args:
            duration_rounded: Target compilation duration
            categorized_videos: Videos categorized by retention percentiles
            
        Returns:
            Selected video document or None if no suitable video found
        """
        # Priority order for video selection
        priority_categories = [
            VideoCategory.TOP_25_PERCENT,
            VideoCategory.SECOND_25_PERCENT,
            VideoCategory.THIRD_25_PERCENT,
            VideoCategory.BOTTOM_25_PERCENT
        ]

        duration_key = f"{duration_rounded}min"

        for category in priority_categories:
            candidates = categorized_videos.get(category, [])

            if not candidates:
                continue

            # Additional filter: ensure videos are within duration limit
            valid_candidates = [
                v for v in candidates
                if v.get('duration_seconds', 0) <= self.MAX_VIDEO_DURATION_SECONDS
            ]

            if not valid_candidates:
                continue

            # First pass: Look for videos never used as first video for this duration
            for video in valid_candidates:
                usage_stats = video.get('compilation_usage_stats', {})
                first_video_by_duration = usage_stats.get(
                    'first_video_by_duration', {})

                # If the field doesn't exist or count is 0, this video can be used
                if first_video_by_duration.get(duration_key, 0) == 0:
                    return video

            # Second pass: If no unused videos found, find the least used one
            # Sort by usage count (ascending) to get least used first
            candidates_with_usage = []
            for video in valid_candidates:
                usage_stats = video.get('compilation_usage_stats', {})
                first_video_by_duration = usage_stats.get(
                    'first_video_by_duration', {})
                usage_count = first_video_by_duration.get(duration_key, 0)

                candidates_with_usage.append((video, usage_count))

            # Sort by usage count, then by retention rate (descending)
            candidates_with_usage.sort(key=lambda x: (
                x[1],  # Usage count (ascending - prefer less used)
                # Retention rate (descending - prefer higher)
                -x[0].get('retention_30s', 0)
            ))

            # Return the best candidate from this category if available
            if candidates_with_usage:
                return candidates_with_usage[0][0]

        # If we get here, no suitable video was found in any category
        return None

    def calculate_compilation_duration_seconds(self, selected_videos: List[Dict]) -> int:
        """
        Calculate the total duration in seconds for selected videos,
        accounting for potential transitions and intro/outro segments.
        
        Args:
            selected_videos: List of selected video documents
            
        Returns:
            Total duration in seconds
        """
        total_duration = 0
        transition_time = 2  # 2 seconds transition between videos

        for video in selected_videos:
            video_duration = video.get('duration_seconds', 0)
            total_duration += video_duration

        # Add transition times (n-1 transitions for n videos)
        if len(selected_videos) > 1:
            total_duration += (len(selected_videos) - 1) * transition_time

        return total_duration

    def select_additional_videos(self, target_duration_minutes: int, first_video: Dict,
                                 categorized_videos: Dict[VideoCategory, List[Dict]]) -> List[Dict]:
        """
        Intelligently select additional videos to fill the compilation duration
        using a sophisticated algorithm that balances content variety and engagement.
        
        Args:
            target_duration_minutes: Target compilation duration in minutes
            first_video: Already selected first video
            categorized_videos: Videos categorized by retention percentiles
            
        Returns:
            List of selected videos including the first video
        """
        selected_videos = [first_video]
        target_duration_seconds = target_duration_minutes * 60
        current_duration = first_video.get('duration_seconds', 0)

        # Create a pool of remaining candidates from all categories
        all_candidates = []
        used_video_ids = {first_video['video_id']}

        # Prioritize higher quality videos but include variety
        category_weights = {
            VideoCategory.TOP_25_PERCENT: 0.4,      # 40% from top tier
            VideoCategory.SECOND_25_PERCENT: 0.35,  # 35% from second tier
            VideoCategory.THIRD_25_PERCENT: 0.20,   # 20% from third tier
            VideoCategory.BOTTOM_25_PERCENT: 0.05   # 5% from bottom tier
        }

        # Build weighted candidate pool
        for category, weight in category_weights.items():
            category_videos = categorized_videos.get(category, [])
            # Filter out already used videos and apply duration constraint
            available_videos = [
                v for v in category_videos
                if v['video_id'] not in used_video_ids and
                v.get('duration_seconds', 0) <= self.MAX_VIDEO_DURATION_SECONDS
            ]

            # Add videos with category weight information
            for video in available_videos:
                video_copy = video.copy()
                video_copy['_selection_weight'] = weight
                all_candidates.append(video_copy)

        # Sort candidates by a combination of duration fit and quality score
        def selection_score(video: Dict) -> float:
            """Calculate selection score for remaining video selection"""
            remaining_duration = target_duration_seconds - current_duration
            video_duration = video.get('duration_seconds', 0)

            # Duration fit score (prefer videos that fit well in remaining time)
            if video_duration <= remaining_duration:
                duration_fit = 1.0 - \
                    abs(remaining_duration - video_duration) / \
                    remaining_duration
            else:
                # Penalty for videos that are too long
                duration_fit = max(
                    0, 1.0 - (video_duration - remaining_duration) / remaining_duration)

            # Quality score based on retention and category
            quality_score = (video.get('retention_30s', 0) ) * video.get('_selection_weight', 0.1)

            # Combined score
            return 0.7 * duration_fit + 0.3 * quality_score

        # Continue selecting videos until target duration is approximately met
        while current_duration < target_duration_seconds * 0.95:  # 95% of target duration
            if not all_candidates:
                break

            # Re-sort candidates based on current state
            available_candidates = [
                v for v in all_candidates
                if v['video_id'] not in used_video_ids and
                v.get('duration_seconds', 0) <= min(
                    self.MAX_VIDEO_DURATION_SECONDS,
                    (target_duration_seconds - current_duration) + 30  # 30s buffer
                )
            ]

            if not available_candidates:
                break

            # Select best fitting video
            available_candidates.sort(key=selection_score, reverse=True)
            selected_video = available_candidates[0]

            selected_videos.append(selected_video)
            used_video_ids.add(selected_video['video_id'])
            current_duration += selected_video.get('duration_seconds', 0)

            # Remove selected video from future consideration
            all_candidates = [
                v for v in all_candidates if v['video_id'] != selected_video['video_id']]

        return selected_videos

    def create_compilation(self, duration_minutes: int, from_date: Optional[str] = None,
                           title_prefix: str = "Auto-Generated", user_id: str = "system") -> Dict:
        """
        Create a new compilation with sophisticated video selection and metadata generation.
        This is the main entry point for compilation creation.
        
        Args:
            duration_minutes: Target duration in minutes (will be rounded to nearest 5)
            from_date: Optional date filter for video selection
            title_prefix: Prefix for the generated compilation title
            user_id: ID of the user creating the compilation
            
        Returns:
            Dictionary containing creation results and compilation metadata
        """
        # Round duration to nearest 5 minutes
        duration_rounded = round(duration_minutes / 5) * 5
        if duration_rounded < 5:
            duration_rounded = 5

        # Get all videos for categorization
        all_videos = list(self.videos_collection.find({}))

        if not all_videos:
            return {
                'success': False,
                'error': 'No videos available in database',
                'compilation_id': None
            }

        # Categorize videos by retention rate with date filtering
        categorized_videos = self.categorize_videos_by_retention(
            all_videos, from_date)

        # Check if any videos are available after filtering
        total_available = sum(len(videos)
                              for videos in categorized_videos.values())
        if total_available == 0:
            return {
                'success': False,
                'error': 'No videos available after applying filters (including 10-minute duration limit)',
                'compilation_id': None
            }

        # Select the first video (highest priority)
        first_video = self.select_first_video(
            duration_rounded, categorized_videos)
        if not first_video:
            return {
                'success': False,
                'error': 'No suitable first video found (under 10 minutes)',
                'compilation_id': None
            }

        # Select additional videos to fill the compilation
        selected_videos = self.select_additional_videos(
            duration_rounded, first_video, categorized_videos)

        if len(selected_videos) < 2:
            return {
                'success': False,
                'error': 'Insufficient videos to create meaningful compilation (under 10 minutes each)',
                'compilation_id': None
            }

        # Calculate actual total duration
        actual_duration_seconds = self.calculate_compilation_duration_seconds(
            selected_videos)

        # Generate compilation metadata
        compilation_doc = self._generate_compilation_document(
            selected_videos, duration_rounded, actual_duration_seconds,
            title_prefix, user_id, from_date
        )

        # Save to database
        try:
            result = self.user_compilations_collection.insert_one(
                compilation_doc)
            compilation_id = str(result.inserted_id)

            return {
                'success': True,
                'compilation_id': compilation_id,
                'selected_videos_count': len(selected_videos),
                'target_duration_minutes': duration_rounded,
                'actual_duration_seconds': actual_duration_seconds,
                'actual_duration_minutes': round(actual_duration_seconds / 60, 1),
                'video_categories_used': self._analyze_category_usage(selected_videos, categorized_videos),
                'max_individual_video_duration': f"{self.MAX_VIDEO_DURATION_SECONDS // 60} minutes"
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Database error: {str(e)}',
                'compilation_id': None
            }

    def _generate_compilation_document(self, selected_videos: List[Dict], duration_rounded: int,
                                       actual_duration_seconds: int, title_prefix: str,
                                       user_id: str, from_date: Optional[str]) -> Dict:
        """
        Generate comprehensive compilation document with all necessary metadata
        """
        # Create timestamps for the compilation
        timestamps = []
        current_time_seconds = 0

        for video in selected_videos:
            # Format timestamp as MM:SS or H:MM:SS
            minutes = current_time_seconds // 60
            seconds = current_time_seconds % 60

            if minutes >= 60:
                hours = minutes // 60
                minutes = minutes % 60
                timestamp = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                timestamp = f"{minutes}:{seconds:02d}"

            timestamps.append({
                'timestamp': timestamp,
                'title': video['title'],
                'video_id': video['video_id'],
                'original_duration': video.get('duration_seconds', 0),
                'original_duration_formatted': f"{video.get('duration_seconds', 0) // 60}:{video.get('duration_seconds', 0) % 60:02d}",
                'retention_rate': video.get('retention_30s', 0)
            })

            # 2s transition
            current_time_seconds += video.get('duration_seconds', 0) + 2

        # Generate intelligent title
        compilation_title = self._generate_compilation_title(
            selected_videos, title_prefix, duration_rounded)

        # Calculate compilation metrics
        avg_retention = sum(v.get('retention_30s', 0)
                            for v in selected_videos) / len(selected_videos)
        total_original_views = sum(v.get('view_count', 0)
                                   for v in selected_videos)

        compilation_doc = {
            'title': compilation_title,
            'duration_rounded': duration_rounded,
            'actual_duration_seconds': actual_duration_seconds,
            'status': CompilationStatus.NOT_PUBLISHED.value,
            'created_by': user_id,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'from_date_filter': from_date,
            'video_count': len(selected_videos),
            'timestamps': timestamps,
            'metadata': {
                'retention_30s': round(avg_retention, 2),
                'total_original_views': total_original_views,
                'creation_algorithm_version': '1.1',  # Updated version for duration constraint
                'max_individual_video_duration_seconds': self.MAX_VIDEO_DURATION_SECONDS,
                'selection_criteria': {
                    'max_annual_usage': self.MAX_ANNUAL_USAGE,
                    'max_video_duration_seconds': self.MAX_VIDEO_DURATION_SECONDS,
                    'retention_weight': self.RETENTION_WEIGHT,
                    'view_count_weight': self.VIEW_COUNT_WEIGHT,
                    'freshness_weight': self.FRESHNESS_WEIGHT
                }
            },
            'export_data': {
                'last_exported': None,
                'export_count': 0
            }
        }

        return compilation_doc

    def _generate_compilation_title(self, videos: List[Dict], prefix: str, duration: int) -> str:
        """Generate intelligent compilation title based on content analysis"""
        # Analyze common themes in video titles
        all_words = []
        for video in videos:
            title_words = video.get('title', '').lower().split()
            all_words.extend(title_words)

        # Find most common meaningful words (exclude common words)
        common_stopwords = {'the', 'a', 'an', 'and', 'or', 'but',
                            'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        meaningful_words = [word for word in all_words if len(
            word) > 2 and word not in common_stopwords]

        word_counts = {}
        for word in meaningful_words:
            word_counts[word] = word_counts.get(word, 0) + 1

        # Get top themes
        top_words = sorted(word_counts.items(),
                           key=lambda x: x[1], reverse=True)[:3]

        if top_words:
            theme = ' '.join([word[0].capitalize() for word in top_words])
            return f"{prefix} - {theme} Collection ({duration} min)"
        else:
            return f"{prefix} - Compilation ({duration} min) - {datetime.now().strftime('%Y%m%d')}"

    def _analyze_category_usage(self, selected_videos: List[Dict],
                                categorized_videos: Dict[VideoCategory, List[Dict]]) -> Dict:
        """Analyze which video categories were used in the final selection"""
        category_usage = {category.value: 0 for category in VideoCategory}

        # Create reverse lookup for video categories
        video_to_category = {}
        for category, videos in categorized_videos.items():
            for video in videos:
                video_to_category[video['video_id']] = category

        # Count usage by category
        for video in selected_videos:
            category = video_to_category.get(video['video_id'])
            if category:
                category_usage[category.value] += 1

        return category_usage

    def get_compilation_stats(self) -> Dict:
        """Get statistics about video eligibility and filtering"""
        all_videos = list(self.videos_collection.find({}))

        stats = {
            'total_videos': len(all_videos),
            'compilation_videos_flagged': 0,
            'compilation_videos_by_title': 0,
            'compilation_videos_in_db': 0,
            'over_10_minutes': 0,
            'over_usage_limit': 0,
            'missing_metrics': 0,
            'eligible_videos': 0
        }

        for video in all_videos:
            # Check for is_compilation flag
            if video.get('is_compilation', False):
                stats['compilation_videos_flagged'] += 1
                continue

            # Check for compilation keywords in title
            video_title = video.get('title', '').lower()
            if any(keyword in video_title for keyword in ['compilation', 'best of', 'highlights', 'collection']):
                stats['compilation_videos_by_title'] += 1
                continue

            # Check if video exists in compilations collection
            if self.compilations_collection.find_one({'video_ids': video.get('video_id')}):
                stats['compilation_videos_in_db'] += 1
                continue

            if video.get('duration_seconds', 0) > self.MAX_VIDEO_DURATION_SECONDS:
                stats['over_10_minutes'] += 1
                continue

            usage_stats = video.get('compilation_usage_stats', {})
            if usage_stats.get('total_inclusions', 0) >= self.MAX_ANNUAL_USAGE:
                stats['over_usage_limit'] += 1
                continue

            if not all(key in video for key in ['retention_30s', 'view_count']):
                stats['missing_metrics'] += 1
                continue

            stats['eligible_videos'] += 1

        return stats

    def get_compilation_preview(self, compilation_id: str) -> Optional[Dict]:
        """Get detailed preview of a created compilation"""
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return None

            # Enrich with additional video details
            enriched_timestamps = []
            for timestamp_entry in compilation.get('timestamps', []):
                video_detail = self.videos_collection.find_one({
                    'video_id': timestamp_entry['video_id']
                })

                if video_detail:
                    enriched_entry = timestamp_entry.copy()
                    enriched_entry['video_details'] = {
                        'view_count': video_detail.get('view_count', 0),
                        'like_count': video_detail.get('like_count', 0),
                        'published_at': video_detail.get('published_at', ''),
                        'thumbnail_url': video_detail.get('thumbnail_url', ''),
                        'full_description': video_detail.get('description', '')[:200] + '...',
                        'duration_check': 'VALID' if video_detail.get('duration_seconds', 0) <= self.MAX_VIDEO_DURATION_SECONDS else 'OVER_LIMIT',
                        'is_compilation': video_detail.get('is_compilation', False),
                        'compilation_check': 'PASS' if not video_detail.get('is_compilation', False) else 'FAIL'
                    }
                    enriched_timestamps.append(enriched_entry)

            compilation['enriched_timestamps'] = enriched_timestamps
            return compilation

        except Exception as e:
            return None

    def update_compilation_status(self, compilation_id: str, new_status: CompilationStatus) -> bool:
        """Update the publication status of a compilation"""
        try:
            result = self.user_compilations_collection.update_one(
                {'_id': ObjectId(compilation_id)},
                {
                    '$set': {
                        'status': new_status.value,
                        'updated_at': datetime.utcnow(),
                        'published_at': datetime.utcnow() if new_status == CompilationStatus.PUBLISHED else None
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            return False

    def delete_compilation(self, compilation_id: str) -> bool:
        """Delete a compilation (only if not published)"""
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return False

            # Only allow deletion of non-published compilations
            if compilation.get('status') == CompilationStatus.PUBLISHED.value:
                return False

            result = self.user_compilations_collection.delete_one({
                '_id': ObjectId(compilation_id)
            })

            return result.deleted_count > 0

        except Exception as e:
            return False

    def get_user_compilations(self, user_id: str = "system", status_filter: Optional[str] = None) -> List[Dict]:
        """Get all compilations created by a specific user with optional status filtering"""
        query = {'created_by': user_id}

        if status_filter:
            query['status'] = status_filter

        compilations = list(self.user_compilations_collection.find(query)
                            .sort('created_at', -1))

        # Convert ObjectId to string for JSON serialization
        for comp in compilations:
            comp['_id'] = str(comp['_id'])

        return compilations
