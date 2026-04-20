"""
4CAT Installation of Selenium gecko webdriver and firefox browser
"""
import subprocess
import argparse
import re
import sys
import os
import shutil
import urllib.request

def find_fourcat_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    while current_dir != os.path.dirname(current_dir):  # Stop at filesystem root
        if os.path.basename(current_dir) == 'config':
            # Found extensions folder, go one level up for 4CAT root
            return os.path.dirname(current_dir)
        current_dir = os.path.dirname(current_dir)
    
    return None

# Ensure the 4CAT root is in the system path
# This is necessary to import common modules like config_manager
fourcat_root = find_fourcat_root()
if fourcat_root and fourcat_root not in sys.path:
    sys.path.insert(0, fourcat_root)
try:
	from common.config_manager import ConfigManager
except ImportError:
	print("Error importing common.config_manager; please ensure you are running this script from the 4CAT root directory.")
	if fourcat_root:
		print(f"DEBUG: 4CAT root at {fourcat_root}")
	exit(1)


if __name__ == "__main__":
	config = ConfigManager()
	cli = argparse.ArgumentParser()
	cli.add_argument("--force", "-f", default=False,
					 help="Force installation of Firefox and Geckodriver even if already installed.",
					 action="store_true")
	cli.add_argument("--component", "-c", default="backend",
					 help="Which component of 4CAT to migrate. Nothing is installed when set to 'frontend'")  # Necessary to work with 4CAT migrate.py
	cli.add_argument("--no-pip", "-p", default=False,
					 help="Run pip to install any python requirements.",
					 action="store_true")
	args, extras = cli.parse_known_args()

	def run_command(command, error_message):
		"""
		Convenence function to run subprocess and check result
		"""
		result = subprocess.run(command.split(" "), stdout=subprocess.PIPE,
							stderr=subprocess.PIPE)

		if result.returncode != 0:
			print(error_message)
			print(command)
			print(result.stdout.decode("ascii"))
			print(result.stderr.decode("ascii"))
			exit(1)

		return result

	def pip_install():
		"""
		Install python requirements
		"""
		print("Installing python requirements")
		interpreter = sys.executable
		command = f"{interpreter} -m pip install -r requirements.txt"
		run_command(command, "Error installing python requirements")

	def install_apt_packages(packages, required=True):
		"""
		Install apt packages, retrying once after apt-get update when package indexes are stale.

		:param str packages: Space-separated list of packages
		:param bool required: If True, installation failure exits. If False, warns and continues.
		:return bool: True if installed successfully, else False
		"""
		command = f"apt-get install --no-install-recommends -y {packages}"
		result = subprocess.run(command.split(" "), stdout=subprocess.PIPE,
							stderr=subprocess.PIPE)

		if result.returncode == 0:
			return True

		stderr = result.stderr.decode("ascii", errors="ignore")
		if "E: Unable to locate package" in stderr:
			print("Packages not found; updating apt-get package list")
			update_result = subprocess.run(["apt-get", "update"], stdout=subprocess.PIPE,
							stderr=subprocess.PIPE)
			if update_result.returncode != 0:
				if required:
					print("Error updating package list")
					print("Please run `apt-get update` manually")
					exit(1)
				print(f"Warning: Could not update apt package list while installing optional package(s): {packages}")
				return False

			result = subprocess.run(command.split(" "), stdout=subprocess.PIPE,
							stderr=subprocess.PIPE)
			if result.returncode == 0:
				return True

		if required:
			print("Error installing packages")
			print("Please install the following packages manually")
			print(packages)
			exit(1)

		print(f"Warning: Could not install optional package(s): {packages}")
		print("Selenium will continue in headless mode unless virtual display is installed later.")
		return False

	if args.component == "frontend":
		# Frontend still needs packages though only to import modules successfully
		if not args.no_pip:
			try:
				import selenium
			except ImportError:
				pip_install()
				print("Installed required packages for extension.")
				exit(0)
		print("4CAT frontend component selected. No installation required.")
		exit(0)
	elif args.component not in ["backend", "both"]:
		print("Invalid component selected. Exiting.")
		exit(1)

	# Check for Linux OS
	if sys.platform != "linux":
		print("This installation is only for Linux OS\nPlease download Firefox and Geckodriver manually.")
		exit(1)
	# Ensure running with root privileges and that APT is available for backend installs
	if args.component in ["backend", "both"]:
		# Require root (Linux only)
		try:
			is_root = (os.geteuid() == 0)
		except AttributeError:
			is_root = False
		if not is_root:
			print("This installer must be run with root privileges (sudo). Please re-run as root or with sudo.")
			exit(1)
		# Check for apt availability
		if shutil.which("apt-get") is None and shutil.which("apt") is None:
			print("APT (apt-get/apt) not found. This installer requires apt on Debian-based systems.")
			print("Please install Firefox and geckodriver manually following Mozilla's instructions: https://packages.mozilla.org/")
			exit(1)

	firefox_installed = False
	geckodriver_installed = False
	xvfb_installed = False
	if not args.force:
		# Check if Firefox, Geckodriver, and Xvfb are already installed
		print("Checking if Firefox, Geckodriver, and Xvfb are already installed")
		firefox_path = shutil.which("firefox")
		if firefox_path is not None:
			command = "firefox --version"
			result = subprocess.run(command.split(" "), stdout=subprocess.PIPE,
									stderr=subprocess.PIPE)

			if result.returncode == 0:
				print("Firefox is already installed")
				firefox_installed = True

		geckodriver_path = shutil.which("geckodriver")
		if geckodriver_path is not None:
			command = "geckodriver --version"
			result = subprocess.run(command.split(" "), stdout=subprocess.PIPE,
									stderr=subprocess.PIPE)

			if result.returncode == 0:
				print("Geckodriver is already installed")
				geckodriver_installed = True

		xvfb_path = shutil.which("Xvfb")
		if xvfb_path is not None:
			print("Xvfb is already installed")
			xvfb_installed = True

	if firefox_installed and geckodriver_installed and xvfb_installed:
		print("Firefox, Geckodriver, and Xvfb already installed. No action required.")
		exit(0)

	# Install additional packages (including fonts and shaping libraries for wide script coverage)
	print("Ensuring required packages are installed")
	PACKAGES = "wget bzip2 libgtk-3-0 libasound2 libdbus-glib-1-2 libx11-xcb1 libxtst6 fontconfig libfreetype6 libharfbuzz0b libpango-1.0-0 fonts-noto-cjk fonts-noto-color-emoji fonts-nanum ttf-wqy-zenhei fonts-dejavu-core fonts-liberation"
	install_apt_packages(PACKAGES, required=True)
	print(f"Installed packages: {PACKAGES}")
	# Rebuild font cache so newly installed fonts are immediately available to Firefox
	try:
		run_command("fc-cache -f -v", "Error rebuilding font cache")
		print("Font cache rebuilt")
	except Exception:
		# continue even if fc-cache fails
		print("Warning: Could not rebuild font cache automatically; you may need to run `fc-cache -f -v` manually.")

	# Attempt to install optional Xvfb (non-fatal if it fails)
	xvfb_available = shutil.which("Xvfb") is not None
	if xvfb_available:
		print("Xvfb is already installed")
	else:
		print("Attempting to install optional package: xvfb")
		if install_apt_packages("xvfb", required=False):
			xvfb_available = shutil.which("Xvfb") is not None
		else:
			xvfb_available = False

	if xvfb_available:
		print("Xvfb is available; virtual display support can be enabled")
	else:
		print("Xvfb is not available; Selenium will use headless mode unless Xvfb is installed later")

	# Identify latest geckodriver
	if not geckodriver_installed:
		print("Identifying latest geckodriver")
		command = "curl -i https://github.com/mozilla/geckodriver/releases/latest"
		geckodriver_github_page = run_command(command, "Error identifying latest geckodriver (curl)")

		match = re.search("v[0-9]+.[0-9]+.[0-9]+", str(geckodriver_github_page.stdout))
		if match:
			GECKODRIVER_VERSION = match.group()
		else:
			print("Error identifying latest geckodriver (regex)")
			exit(1)

		# Download and set up geckodriver
		print(f'Installing geckodriver version {GECKODRIVER_VERSION}')
		command = f"wget https://github.com/mozilla/geckodriver/releases/download/{GECKODRIVER_VERSION}/geckodriver-{GECKODRIVER_VERSION}-linux64.tar.gz"
		run_command(command, "Error downloading geckodriver")

		command = f"tar -zxf geckodriver-{GECKODRIVER_VERSION}-linux64.tar.gz -C /usr/local/bin"
		run_command(command, "Error unziping geckodriver")

		command = "chmod +x /usr/local/bin/geckodriver"
		run_command(command, "Error changing ownership of geckodriver")

		command = f"rm geckodriver-{GECKODRIVER_VERSION}-linux64.tar.gz"
		run_command(command, "Error removing temp download files")

	# Install latest firefox via Mozilla APT repository (Debian-based recommended method)
	# See: https://packages.mozilla.org/ for the official instructions and signing key
	if not firefox_installed:
		print("Installing the latest version of Firefox")
		# https://support.mozilla.org/en-US/kb/install-firefox-linux
		command = "apt-get purge -y firefox"
		run_command(command, "Error removing existing firefox")

		# Ensure the apt keyrings directory exists
		keyrings_dir = "/etc/apt/keyrings"
		try:
			os.makedirs(keyrings_dir, mode=0o755, exist_ok=True)
		except Exception as e:
			print(f"Warning: Could not create {keyrings_dir}: {e}")

		# Import Mozilla APT repository signing key
		key_url = "https://packages.mozilla.org/apt/repo-signing-key.gpg"
		key_path = os.path.join(keyrings_dir, "packages.mozilla.org.asc")
		try:
			with urllib.request.urlopen(key_url) as resp:
				key_data = resp.read()
			with open(key_path, "wb") as kf:
				kf.write(key_data)
			print(f"Imported Mozilla APT repository signing key to {key_path}")
		except Exception as e:
			print(f"Error importing Mozilla signing key: {e}")
			exit(1)

		# Add Mozilla APT repository (both modern .sources and legacy .list for compatibility)
		mozilla_sources = "/etc/apt/sources.list.d/mozilla.sources"
		mozilla_list = "/etc/apt/sources.list.d/mozilla.list"
		try:
			sources_content = (
				"Types: deb\n"
				"URIs: https://packages.mozilla.org/apt\n"
				"Suites: mozilla\n"
				"Components: main\n"
				"Signed-By: /etc/apt/keyrings/packages.mozilla.org.asc\n"
			)
			with open(mozilla_sources, "w") as sf:
				sf.write(sources_content)
			print(f"Wrote {mozilla_sources}")
		except Exception as e:
			print(f"Warning: Could not write {mozilla_sources}: {e}")

		try:
			deb_line = "deb [signed-by=/etc/apt/keyrings/packages.mozilla.org.asc] https://packages.mozilla.org/apt mozilla main\n"
			existing = ""
			if os.path.exists(mozilla_list):
				with open(mozilla_list, "r", encoding="utf-8") as lf:
					existing = lf.read()
			if deb_line.strip() not in existing:
				with open(mozilla_list, "a", encoding="utf-8") as lf:
					lf.write(deb_line)
			print(f"Wrote {mozilla_list}")
		except Exception as e:
			print(f"Warning: Could not write {mozilla_list}: {e}")

		# Prioritize packages from Mozilla repo
		try:
			pref_path = "/etc/apt/preferences.d/mozilla"
			pref_content = (
				"Package: *\n"
				"Pin: origin packages.mozilla.org\n"
				"Pin-Priority: 1000\n"
			)
			with open(pref_path, "w", encoding="utf-8") as pf:
				pf.write(pref_content)
			print(f"Wrote {pref_path}")
		except Exception as e:
			print(f"Warning: Could not write apt preferences: {e}")

		# Update and install firefox
		print("Updating apt package list and installing firefox")
		run_command("apt-get update", "Error updating package list")
		run_command("apt-get install -y firefox", "Error installing firefox")

	if not args.no_pip:
		pip_install()

	config.with_db()
	config.set('selenium.selenium_executable_path', "/usr/local/bin/geckodriver")
	config.set('selenium.browser', 'firefox')
	config.set('selenium.use_virtual_display', bool(xvfb_available))
	print("Firefox and Geckodriver installation complete")
	exit(0)
