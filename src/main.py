import tkinter as tk
from tkinter import filedialog
from datetime import datetime

from crawler import crawl

def run_gui():
    root = tk.Tk()
    root.withdraw()
    print("Select Source (Base Path)")
    base_path = filedialog.askdirectory(title="Select Source Directory")
    print("elect Output (Database Path)")
    output_path = filedialog.askdirectory(title="Select Output Directory")
    today_str = datetime.now().strftime("%Y-%m-%d")
    crawl(base_path, today_str, output_path)

if __name__ == "__main__":
    run_gui()
