#!/usr/bin/env bash
#
# Blacksmith installation script
# Usage: curl -fsSL https://raw.githubusercontent.com/ozten/blacksmith/main/scripts/install.sh | bash
#
# Set BLACKSMITH_VERSION to install a specific version:
#   BLACKSMITH_VERSION=0.1.0 curl -fsSL ... | bash
#

set -e

REPO="ozten/blacksmith"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}==>${NC} $1"; }
log_success() { echo -e "${GREEN}==>${NC} $1"; }
log_warning() { echo -e "${YELLOW}==>${NC} $1"; }
log_error()   { echo -e "${RED}Error:${NC} $1" >&2; }

detect_platform() {
    local os arch

    case "$(uname -s)" in
        Darwin) os="darwin" ;;
        Linux)  os="linux" ;;
        *)
            log_error "Unsupported operating system: $(uname -s)"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)   arch="amd64" ;;
        aarch64|arm64)  arch="arm64" ;;
        *)
            log_error "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac

    echo "${os}_${arch}"
}

fetch() {
    local url=$1
    if command -v curl &>/dev/null; then
        curl -fsSL "$url"
    elif command -v wget &>/dev/null; then
        wget -qO- "$url"
    else
        log_error "Neither curl nor wget found."
        exit 1
    fi
}

download() {
    local url=$1 dest=$2
    if command -v curl &>/dev/null; then
        curl -fsSL -o "$dest" "$url"
    else
        wget -q -O "$dest" "$url"
    fi
}

# Re-sign binary for macOS to avoid slow Gatekeeper checks
resign_for_macos() {
    [[ "$(uname -s)" != "Darwin" ]] && return 0
    command -v codesign &>/dev/null || return 0

    log_info "Re-signing binary for macOS..."
    codesign --remove-signature "$1" 2>/dev/null || true
    codesign --force --sign - "$1" 2>/dev/null && log_success "Binary re-signed" || true
}

main() {
    echo ""
    echo "Blacksmith Installer"
    echo ""

    local platform
    platform=$(detect_platform)
    log_info "Platform: $platform"

    # Determine version
    local version release_json
    if [[ -n "${BLACKSMITH_VERSION:-}" ]]; then
        version="v${BLACKSMITH_VERSION#v}"
        log_info "Requested version: $version"
        release_json=$(fetch "https://api.github.com/repos/${REPO}/releases/tags/${version}")
    else
        log_info "Fetching latest release..."
        release_json=$(fetch "https://api.github.com/repos/${REPO}/releases/latest")
        version=$(echo "$release_json" | grep '"tag_name"' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')
    fi

    if [[ -z "$version" ]]; then
        log_error "Failed to determine version"
        exit 1
    fi

    log_info "Installing blacksmith $version"

    local version_num="${version#v}"
    local archive="blacksmith_${version_num}_${platform}.tar.gz"

    # Check asset exists
    if ! echo "$release_json" | grep -Fq "\"name\": \"$archive\""; then
        log_error "No prebuilt binary for $platform in release $version"
        echo ""
        echo "Available at: https://github.com/${REPO}/releases/tag/${version}"
        exit 1
    fi

    local download_url="https://github.com/${REPO}/releases/download/${version}/${archive}"
    local tmp_dir
    tmp_dir=$(mktemp -d)

    log_info "Downloading $archive..."
    download "$download_url" "$tmp_dir/$archive"

    log_info "Extracting..."
    tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"

    # Determine install location
    local install_dir="/usr/local/bin"
    if [[ ! -w "$install_dir" ]]; then
        install_dir="$HOME/.local/bin"
        mkdir -p "$install_dir"
    fi

    log_info "Installing to $install_dir..."

    for bin in blacksmith blacksmith-ui; do
        if [[ -f "$tmp_dir/$bin" ]]; then
            chmod +x "$tmp_dir/$bin"
            if [[ -w "$install_dir" ]]; then
                mv "$tmp_dir/$bin" "$install_dir/"
            else
                sudo mv "$tmp_dir/$bin" "$install_dir/"
            fi
            resign_for_macos "$install_dir/$bin"
        fi
    done

    rm -rf "$tmp_dir"

    log_success "blacksmith installed to $install_dir/blacksmith"

    # PATH check
    if [[ ":$PATH:" != *":$install_dir:"* ]]; then
        log_warning "$install_dir is not in your PATH"
        echo ""
        echo "Add to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo "  export PATH=\"\$PATH:$install_dir\""
        echo ""
    fi

    # Verify
    if command -v blacksmith &>/dev/null; then
        log_success "Installation complete!"
        echo ""
        blacksmith --version 2>/dev/null || true
    else
        log_warning "Installed but 'blacksmith' not found in PATH yet. Open a new shell or update PATH."
    fi
}

main "$@"
