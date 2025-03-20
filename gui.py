import customtkinter as ctk
from tkinter import scrolledtext
import subprocess
import threading
import sys
import os
import json
import time
from queue import Queue

# Configure appearance
ctk.set_appearance_mode("dark")  # Choose "light", "dark", or "system"
ctk.set_default_color_theme("blue")  # Choose "blue", "green", "dark-blue", etc.

# Thread-safe queue for logging
log_queue = Queue()


def format_time(seconds: float) -> str:
    """Convert seconds to a human-readable format (HH:MM:SS)"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class ProgressMonitor(threading.Thread):
    def __init__(self, progress_bar, progress_label):
        super().__init__(daemon=True)
        self.progress_bar = progress_bar
        self.progress_label = progress_label
        self.running = True

    def run(self):
        while self.running:
            try:
                if os.path.exists("scrape_progress.json"):
                    with open("scrape_progress.json", "r") as f:
                        progress = json.load(f)
                        total = progress["total_items"]
                        to_scrape = progress["items_to_scrape"]
                        processed = progress["processed"]
                        failed = progress["failed"]
                        successful = progress["successful"]
                        remaining_time = progress.get("estimated_remaining_time", 0)

                        if to_scrape == 0:
                            percentage = 0
                        else:
                            percentage = processed / to_scrape

                        self.progress_bar.configure(mode="determinate")
                        self.progress_bar.set(percentage)
                        self.progress_label.configure(
                            text=f"Total: {total}, To Scrape: {to_scrape}\n"
                                 f"Processed: {processed} ({percentage:.1%})\n"
                                 f"Failed: {failed}, Successful: {successful}\n"
                                 f"Time Remaining: {format_time(remaining_time)}"
                        )
                else:
                    self.progress_label.configure(text="Initializing scraper...")
            except Exception as e:
                pass  # Silently handle transient read errors
            time.sleep(0.5)  # Update every 500ms

    def stop(self):
        self.running = False

# Function to run the scraper
def run_scraper():
    clear_logs()
    log_message("Running ScrapePrices...\n")
    run_script("ScrapePrices.py")


# Function to update the spreadsheet
def update_spreadsheet():
    clear_logs()
    log_message("Updating spreadsheet with SheetLoad...\n")
    run_script("SheetLoad.py")


# Function to pull data from the spreadsheet
def pull_spreadsheet():
    clear_logs()
    log_message("Pulling data with PullSheet...\n")
    run_script("PullSheet.py")


# Function to calculate total value
def calculate_total():
    clear_logs()
    log_message("Calculating total value with TotalCost...\n")
    run_script("TotalCost.py")


# Function to clear logs
def clear_logs():
    logging_window.configure(state="normal")
    logging_window.delete(1.0, "end")
    logging_window.configure(state="disabled")


# Function to run a script with real-time output
def run_script(script_name):
    def target():
        monitor = None
        try:
            # Initialize progress components
            progress_bar.set(0)
            progress_bar.configure(mode="determinate")
            progress_label.configure(text="Starting...")

            # Start progress monitoring
            monitor = ProgressMonitor(progress_bar, progress_label)
            monitor.start()

            process = subprocess.Popen(
                [sys.executable, "-u", script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=os.path.dirname(os.path.abspath(__file__)),
                bufsize=1,
                env={**os.environ, "PYTHONUNBUFFERED": "1"}
            )

            def read_stream(stream, prefix=""):
                for line in iter(stream.readline, ''):
                    line = line.rstrip()
                    if line:
                        log_message(f"{prefix}{line}\n")

            # Start threads for stdout/stderr
            stdout_thread = threading.Thread(
                target=read_stream, args=(process.stdout,), daemon=True
            )
            stderr_thread = threading.Thread(
                target=read_stream, args=(process.stderr, "ERROR: "), daemon=True
            )
            stdout_thread.start()
            stderr_thread.start()

            process.wait()
            stdout_thread.join()
            stderr_thread.join()

            if process.returncode == 0:
                log_message(f"{script_name} completed successfully!\n")
            else:
                log_message(f"{script_name} failed with error code {process.returncode}.\n")
        except Exception as e:
            log_message(f"Error running {script_name}: {e}\n")
        finally:
            if monitor:
                monitor.stop()
            progress_bar.stop()
            progress_bar.set(0)
            progress_label.configure(text="")
            # Cleanup progress file if leftover
            if os.path.exists("scrape_progress.json"):
                try:
                    os.remove("scrape_progress.json")
                except:
                    pass

    threading.Thread(target=target, daemon=True).start()


# Thread-safe logging
def log_message(message):
    log_queue.put(message)
    root.after(10, process_log_queue)  # Faster refresh (10ms)


# Process the queue in the main thread
def process_log_queue():
    while not log_queue.empty():
        message = log_queue.get()
        logging_window.configure(state="normal")
        logging_window.insert("end", message)
        logging_window.configure(state="disabled")
        logging_window.see("end")


# Function to exit the program
def exit_program():
    root.quit()


# Function to show help
def show_help():
    ctk.CTkMessageBox(title="Help", message="This is the MTG Booster Scraper GUI.\n\n"
                                            "1. Run Scraper: Collects prices from TCGPlayer.\n"
                                            "2. Update Spreadsheet: Updates the Google Spreadsheet with scraped data.\n"
                                            "3. Pull Spreadsheet: Pulls data from the spreadsheet to a JSON file.\n"
                                            "4. Calculate Total: Calculates the total value of scraped prices.\n\n"
                                            "Logs are displayed in the window below.")


# Create the main window
root = ctk.CTk()
root.title("MTG Booster Scraper")
root.geometry("800x600")

# Add a label
label = ctk.CTkLabel(root, text="MTG Booster Scraper", font=("Arial", 24, "bold"))
label.pack(pady=20)

# Add buttons
button_frame = ctk.CTkFrame(root)
button_frame.pack(pady=10)

scraper_button = ctk.CTkButton(button_frame, text="Run Scraper", command=run_scraper, width=200, fg_color="#4CAF50",
                               hover_color="#45a049")
scraper_button.grid(row=0, column=0, padx=10, pady=10)

update_button = ctk.CTkButton(button_frame, text="Update Spreadsheet", command=update_spreadsheet, width=200,
                              fg_color="#2196F3", hover_color="#1e88e5")
update_button.grid(row=0, column=1, padx=10, pady=10)

pull_button = ctk.CTkButton(button_frame, text="Pull Spreadsheet", command=pull_spreadsheet, width=200,
                            fg_color="#9C27B0", hover_color="#8e24aa")
pull_button.grid(row=1, column=0, padx=10, pady=10)

total_button = ctk.CTkButton(button_frame, text="Calculate Total", command=calculate_total, width=200,
                             fg_color="#FF9800", hover_color="#fb8c00")
total_button.grid(row=1, column=1, padx=10, pady=10)

# Add a progress bar
progress_bar = ctk.CTkProgressBar(root, width=600)
progress_bar.pack(pady=10)
progress_bar.set(0)

# Add a progress label
progress_label = ctk.CTkLabel(root, text="", font=("Arial", 12))
progress_label.pack(pady=5)

# Add a logging window
logging_window = scrolledtext.ScrolledText(root, state="disabled", width=90, height=20, font=("Consolas", 12))
logging_window.pack(pady=20)

# Add a menu bar
menu_frame = ctk.CTkFrame(root)
menu_frame.pack(fill="x", pady=5)

file_menu = ctk.CTkButton(menu_frame, text="File", width=50, fg_color="transparent", hover_color="#2e2e2e",
                          command=exit_program)
file_menu.pack(side="left", padx=5)

help_menu = ctk.CTkButton(menu_frame, text="Help", width=50, fg_color="transparent", hover_color="#2e2e2e",
                          command=show_help)
help_menu.pack(side="left", padx=5)

# Start the GUI
root.mainloop()