#!/usr/bin/env bash
# Encodes the load test's publisher media from Big Buck Bunny: the first
# minute, at 24, 30 and 60 fps, in the three simulcast rungs the publisher
# sends (720p, 360p, 180p), in H.264 and H.265, with the audio beside each set.
#
#   thirdparty/sfu/media/bbb/encode.sh ~/Downloads/big_buck_bunny_720p_h264.mov
#
# FPS_SETS limits it to some of the sets, SECONDS_OF_SOURCE changes the minute.
#
# Writes <fps>/output-{720p,360p,180p}.{h264,h265} and <fps>/output.ogg next to
# this script: the names the publisher reads, so a set is chosen with
# `loadtest -media-dir=.../bbb/<fps> -fps=<fps>`.
#
# What the files have to be, for the publisher to send them as a phone would:
#   - Annex B elementary streams, parameter sets repeated at every keyframe, so
#     a viewer joining mid-stream can decode from the next one.
#   - No B-frames: WebRTC sends frames in decode order and a phone's decoder
#     is set up for none.
#   - A keyframe every 2 seconds and never elsewhere (no scene cuts), so the
#     cost of keyframes is the same in every run, and every one an IDR: x265's
#     default open GOP makes them CRA pictures, which a phone recovering from
#     loss or joining mid-stream may not start decoding from.
#   - H.264 Constrained Baseline, which is what the publisher advertises
#     (profile-level-id 42e01f) and every WebRTC decoder takes.
#   - Opus at 48kHz stereo in 20ms packets, one per Ogg page: the publisher
#     sends each page as one sample.
#
# 30 and 60 fps are motion-interpolated from the 24 fps source rather than made
# by repeating frames: a 2-3-2-3 repeat at 60 fps is judder, and judder in the
# source reads as the stutter these runs look for. Interpolation is slow and
# single-threaded; the interpolated minute is kept in work/ and reused.
set -euo pipefail

src="${1:?usage: $0 <big_buck_bunny_720p_h264.mov>}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
seconds="${SECONDS_OF_SOURCE:-60}"
# Which sets to make; FPS_SETS=24 makes only the native-rate one.
read -r -a sets <<<"${FPS_SETS:-24 30 60}"
work="$here/work"
mkdir -p "$work"

# rung  width  height  kbps at 24/30/60 fps: h264 then h265
rungs=(
	"720p 1280 720 2000 2500 3500 1200 1500 2100"
	"360p 640  360 700  850  1200 400  500  700"
	"180p 320  180 250  300  450  150  180  260"
)

ff() { ffmpeg -hide_banner -loglevel error -nostdin -y "$@"; }

# The source minute at each frame rate, near-lossless, to encode the rungs from.
intermediate() {
	local fps="$1" out="$work/source-$1.mkv"
	[[ -f "$out" ]] && return
	local filter="setpts=PTS-STARTPTS"
	if [[ "$fps" != 24 ]]; then
		filter="$filter,minterpolate=fps=$fps:mi_mode=mci:mc_mode=aobmc:me_mode=bidir:vsbmc=1"
	fi
	echo "==> $fps fps source minute"
	ff -i "$src" -t "$seconds" -map 0:v:0 -an -vf "$filter" \
		-c:v libx264 -preset veryfast -crf 10 -pix_fmt yuv420p "$out.tmp.mkv"
	mv "$out.tmp.mkv" "$out"
}

encode() {
	local fps="$1" name="$2" width="$3" height="$4" h264="$5" h265="$6"
	local dir="$here/$fps" gop=$((fps * 2))
	local scale="scale=$width:$height:flags=lanczos"
	mkdir -p "$dir"

	# 720p60 is past level 3.1; the SDP's level-asymmetry-allowed covers it.
	local level=3.1
	if ((width * height * fps > 1280 * 720 * 30)); then level=3.2; fi

	ff -i "$work/source-$fps.mkv" -vf "$scale" -r "$fps" \
		-c:v libx264 -preset medium -profile:v baseline -level "$level" -pix_fmt yuv420p \
		-b:v "${h264}k" -maxrate "${h264}k" -bufsize "${h264}k" \
		-g "$gop" -keyint_min "$gop" -sc_threshold 0 -bf 0 \
		-x264-params "repeat-headers=1" \
		-f h264 "$dir/output-$name.h264"

	ff -i "$work/source-$fps.mkv" -vf "$scale" -r "$fps" \
		-c:v libx265 -preset medium -profile:v main -pix_fmt yuv420p \
		-b:v "${h265}k" -maxrate "${h265}k" -bufsize "${h265}k" \
		-x265-params "log-level=error:bframes=0:keyint=$gop:min-keyint=$gop:scenecut=0:open-gop=0:repeat-headers=1:vbv-maxrate=$h265:vbv-bufsize=$h265" \
		-f hevc "$dir/output-$name.h265"
}

echo "==> audio"
ff -i "$src" -t "$seconds" -map 0:a:0 -ac 2 -ar 48000 \
	-c:a libopus -b:a 64k -frame_duration 20 -page_duration 20000 \
	"$work/output.ogg"

# Side by side: interpolation uses one core, and the three are independent.
for fps in "${sets[@]}"; do intermediate "$fps" & done
wait

for fps in "${sets[@]}"; do
	i=$(( fps == 24 ? 0 : fps == 30 ? 1 : 2 ))
	for rung in "${rungs[@]}"; do
		read -r name width height a24 a30 a60 b24 b30 b60 <<<"$rung"
		h264=( "$a24" "$a30" "$a60" ); h265=( "$b24" "$b30" "$b60" )
		echo "==> $fps fps $name"
		encode "$fps" "$name" "$width" "$height" "${h264[$i]}" "${h265[$i]}"
	done
	cp "$work/output.ogg" "$here/$fps/output.ogg"
done

echo "==> done; $work holds the intermediates and can be deleted"
