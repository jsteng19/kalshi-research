/**
 * Fast Audio Capture from Browser
 * 
 * Captures audio from DRM-protected video at high playback speeds.
 * Maintains full audio quality regardless of playback speed.
 * 
 * Usage:
 * 1. Open the video in your browser
 * 2. Open Developer Console (F12 or Cmd+Option+I)
 * 3. Paste this entire script and press Enter
 * 4. Video will play at high speed while recording audio
 * 5. When complete, audio file will auto-download
 */

(async function() {
    console.log('🎙️ Fast Audio Capture Starting...');
    
    // Configuration
    const PLAYBACK_SPEED = 1.0;  // 4x speed (adjust as needed: 2.0, 4.0, 8.0, 16.0)
    const OUTPUT_FILENAME = 'captured_audio.webm';
    
    // Find video element
    const video = document.querySelector('video');
    if (!video) {
        console.error('❌ No video element found on page');
        return;
    }
    
    console.log(`✅ Video found: ${video.duration ? Math.floor(video.duration/60) : '?'} minutes`);
    console.log(`⚡ Setting playback speed to ${PLAYBACK_SPEED}x`);
    console.log(`⏱️  Estimated capture time: ${video.duration ? Math.floor(video.duration/PLAYBACK_SPEED/60) : '?'} minutes`);
    
    // Set playback speed and force it to stay
    video.playbackRate = PLAYBACK_SPEED;
    
    // Prevent player from resetting playback speed
    let targetSpeed = PLAYBACK_SPEED;
    const enforceSpeed = setInterval(() => {
        if (Math.abs(video.playbackRate - targetSpeed) > 0.1) {
            video.playbackRate = targetSpeed;
        }
    }, 100); // Check every 100ms
    
    // Prevent player from seeking/jumping around
    let lastTime = video.currentTime;
    const preventJumps = setInterval(() => {
        const timeDiff = Math.abs(video.currentTime - lastTime);
        // If jumped more than 2 seconds (and not due to user seeking)
        if (timeDiff > 2 && timeDiff < video.duration - 5) {
            console.log(`⚠️ Player tried to jump ${timeDiff.toFixed(1)}s - preventing`);
            // Don't prevent it, just log it - the audio capture will continue
        }
        lastTime = video.currentTime;
    }, 500);
    
    // Store interval IDs for cleanup
    window.speedEnforcer = enforceSpeed;
    window.jumpPreventer = preventJumps;
    
    // Setup audio capture
    const audioContext = new AudioContext();
    const destination = audioContext.createMediaStreamDestination();
    
    // Connect video audio to recorder
    // Use captureStream if available (avoids the "already associated" error)
    let audioStream;
    try {
        // Try modern approach first (doesn't require createMediaElementSource)
        if (video.captureStream) {
            console.log('✅ Using video.captureStream() method');
            audioStream = video.captureStream();
        } else if (video.mozCaptureStream) {
            console.log('✅ Using video.mozCaptureStream() method (Firefox)');
            audioStream = video.mozCaptureStream();
        } else {
            throw new Error('captureStream not supported');
        }
        
        // Connect the captured stream
        const audioTracks = audioStream.getAudioTracks();
        if (audioTracks.length === 0) {
            console.error('❌ No audio tracks found in video stream');
            return;
        }
        console.log(`✅ Found ${audioTracks.length} audio track(s)`);
        
    } catch (e) {
        console.log('⚠️ captureStream not available, trying MediaElementSource...');
        try {
            const source = audioContext.createMediaElementSource(video);
            source.connect(destination);
            source.connect(audioContext.destination);
            audioStream = destination.stream;
        } catch (err) {
            console.error('❌ Error: ' + err.message);
            console.error('💡 Solution: Refresh the page and try again (video element may be in use)');
            return;
        }
    }
    
    // Create recorder with timeslice to ensure data is captured frequently
    const mediaRecorder = new MediaRecorder(audioStream, {
        mimeType: 'audio/webm;codecs=opus',
        audioBitsPerSecond: 128000
    });
    
    const chunks = [];
    mediaRecorder.ondataavailable = e => {
        if (e.data.size > 0) {
            chunks.push(e.data);
            console.log(`📦 Captured chunk: ${(e.data.size / 1024).toFixed(1)} KB (Total: ${chunks.length} chunks)`);
        }
    };
    
    // Handle recording completion
    mediaRecorder.onstop = () => {
        console.log('💾 Saving audio file...');
        console.log(`📦 Total chunks captured: ${chunks.length}`);
        
        if (chunks.length === 0) {
            console.error('❌ No audio data captured! Possible issues:');
            console.error('   - Video may not have audio track');
            console.error('   - Browser may be blocking audio capture');
            console.error('   - Video needs to play for at least a few seconds');
            return;
        }
        
        const blob = new Blob(chunks, { type: 'audio/webm' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = OUTPUT_FILENAME;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        const sizeMB = (blob.size / (1024 * 1024)).toFixed(1);
        console.log(`✅ Audio saved: ${OUTPUT_FILENAME} (${sizeMB} MB)`);
        console.log('🔄 Convert to MP3 with: ffmpeg -i captured_audio.webm -acodec libmp3lame -b:a 128k output.mp3');
    };
    
    // Auto-stop when video ends
    video.addEventListener('ended', () => {
        console.log('🏁 Video ended, stopping recording...');
        clearInterval(enforceSpeed);
        clearInterval(preventJumps);
        mediaRecorder.stop();
    });
    
    // Start recording with timeslice (capture data every 1 second)
    mediaRecorder.start(1000); // Request data every 1000ms
    console.log('🎙️ Recording started!');
    console.log('📊 Progress updates every 30 seconds...');
    console.log('💡 Audio chunks will be captured every second');
    
    // Play video if not already playing
    if (video.paused) {
        video.play();
    }
    
    // Progress indicator
    const progressInterval = setInterval(() => {
        if (video.duration) {
            const progress = (video.currentTime / video.duration * 100).toFixed(1);
            const remaining = (video.duration - video.currentTime) / PLAYBACK_SPEED;
            console.log(`📊 Progress: ${progress}% | Remaining: ${Math.floor(remaining/60)}m ${Math.floor(remaining%60)}s`);
        }
    }, 30000);
    
    // Cleanup on completion
    video.addEventListener('ended', () => {
        clearInterval(progressInterval);
    }, { once: true });
    
    // Manual stop function
    window.stopRecording = () => {
        console.log('⏹️ Manually stopping recording...');
        clearInterval(progressInterval);
        clearInterval(enforceSpeed);
        clearInterval(preventJumps);
        mediaRecorder.stop();
        video.pause();
    };
    
    // Function to change speed on the fly
    window.setSpeed = (speed) => {
        targetSpeed = speed;
        video.playbackRate = speed;
        console.log(`⚡ Speed changed to ${speed}x`);
    };
    
    console.log('');
    console.log('⚙️ Controls:');
    console.log('   - To stop early: stopRecording()');
    console.log('   - To change speed: setSpeed(8.0)  [enforces the speed]');
    console.log('   - Check capture status: audioChunks.length');
    console.log('   - Current speed: ' + video.playbackRate + 'x');
    console.log('');
    console.log('🛡️ Speed enforcement active - player cannot override your speed');
    console.log('⚠️  IMPORTANT: Let video play for at least 10-15 seconds before stopping!');
    console.log('   Skipping to the end immediately may result in empty file.');
    
    // Make chunks accessible for debugging
    window.audioChunks = chunks;
    
})();

