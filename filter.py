# ROS2 Bag Filter GUI
# This script provides a GUI for filtering ROS2 bag files based on selected topics and time ranges.
# It allows users to select an input bag file, specify an output directory, and choose topics to include in the output.
# The script uses the rosbag2_py library to read and write bag files, and tkinter for the GUI.
# Author: Fengze
# Date: 2025-05-06
# License: GNU General Public License v3.0


import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import sqlite3

from rosbag2_py import (
    SequentialReader,
    SequentialWriter,
    StorageOptions,
    ConverterOptions,
    TopicMetadata,
    StorageFilter,
)

class CheckboxListbox(tk.Listbox):
    """Listbox with checkbox-like single-click selection"""
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.bind('<Button-1>', self.toggle_selection)
        
    def toggle_selection(self, event):
        item = self.nearest(event.y)
        current_state = self.selection_includes(item)
        self.selection_set(item) if not current_state else self.selection_clear(item)

class ROS2BagFilterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ROS2 Bag Filter")
        self.available_topics = {}
        self.topic_names = []
        
        self.create_widgets()
        self.grid_config()
        self.setup_defaults()

    def create_widgets(self):
        # Input bag selection
        ttk.Label(self.root, text="Input Bag:").grid(row=0, column=0, sticky="w")
        self.input_path = tk.StringVar()
        self.input_entry = ttk.Entry(self.root, textvariable=self.input_path, width=50)
        self.input_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(self.root, text="Browse", command=self.browse_input).grid(row=0, column=2, padx=5)

        # Output directory selection
        ttk.Label(self.root, text="Output Directory:").grid(row=1, column=0, sticky="w")
        self.output_path = tk.StringVar()
        self.output_entry = ttk.Entry(self.root, textvariable=self.output_path, width=50)
        self.output_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(self.root, text="Browse", command=self.browse_output).grid(row=1, column=2, padx=5)

        # Topics list with checkboxes
        ttk.Label(self.root, text="Select Topics:").grid(row=2, column=0, sticky="nw")
        self.topic_list = CheckboxListbox(
            self.root,
            selectmode=tk.MULTIPLE,
            height=10,
            bg="white",
            selectbackground="#4A6984",
            selectforeground="white",
            activestyle='none'
        )
        self.topic_list.grid(row=2, column=1, padx=5, pady=5, sticky="nsew")

        # Selection controls
        btn_frame = ttk.Frame(self.root)
        btn_frame.grid(row=2, column=2, sticky="ns", padx=5)
        ttk.Button(btn_frame, text="Select All", command=self.select_all).pack(pady=2)
        ttk.Button(btn_frame, text="Deselect All", command=self.deselect_all).pack(pady=2)

        # Time filters
        ttk.Label(self.root, text="Start Time (s):").grid(row=3, column=0, sticky="w")
        self.start_time = ttk.Entry(self.root, width=15)
        self.start_time.grid(row=3, column=1, sticky="w", padx=5, pady=5)

        ttk.Label(self.root, text="End Time (s):").grid(row=4, column=0, sticky="w")
        self.end_time = ttk.Entry(self.root, width=15)
        self.end_time.grid(row=4, column=1, sticky="w", padx=5, pady=5)

        # Process button
        self.process_btn = ttk.Button(self.root, text="Process Bag", command=self.process_bag)
        self.process_btn.grid(row=5, column=1, pady=10)

    def grid_config(self):
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(2, weight=1)

    def setup_defaults(self):
        self.start_time.insert(0, "0.0")
        self.end_time.insert(0, "")

    def select_all(self):
        self.topic_list.selection_set(0, tk.END)

    def deselect_all(self):
        self.topic_list.selection_clear(0, tk.END)

    def browse_input(self):
        path = filedialog.askdirectory(title="Select Input Bag Directory")
        if path:
            self.input_path.set(path)
            self.load_metadata()

    def browse_output(self):
        path = filedialog.askdirectory(title="Select Output Directory")
        if path:
            self.output_path.set(path)

    def load_metadata(self):
        try:
            bag_dir = self.input_path.get()
            if not bag_dir:
                return

            db_file = self.find_sqlite_file(bag_dir)
            conn = sqlite3.connect(os.path.join(bag_dir, db_file))
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.name, t.type, COUNT(m.topic_id)
                FROM topics t
                LEFT JOIN messages m ON t.id = m.topic_id
                GROUP BY t.id
            """)
            
            self.available_topics = {}
            self.topic_names = []
            self.topic_list.delete(0, tk.END)
            
            for name, type_, count in cursor.fetchall():
                display_text = f"{name} ({type_}) [{count} messages]"
                self.topic_list.insert(tk.END, display_text)
                self.topic_names.append(name)
                self.available_topics[name] = (type_, count)
            
            conn.close()
            
            if self.topic_list.size() > 0 and not self.topic_list.curselection():
                self.topic_list.selection_set(0)
                
        except Exception as e:
            messagebox.showerror("Error", f"Metadata error: {str(e)}")

    def find_sqlite_file(self, bag_dir):
        for file in os.listdir(bag_dir):
            if file.endswith('.db3'):
                return file
        raise FileNotFoundError("No SQLite database found in bag directory")

    def process_bag(self):
        if not self.validate_inputs():
            return

        try:
            selected_indices = self.topic_list.curselection()
            if not selected_indices:
                messagebox.showerror("Error", "No topics selected!")
                return
                
            selected_topics = [self.topic_names[i] for i in selected_indices]
            start_offset, end_offset = self.get_time_offsets()
            min_ts = self.get_min_timestamp()

            start_ns = min_ts + int(start_offset * 1e9)
            end_ns = min_ts + int(end_offset * 1e9) if end_offset != float('inf') else float('inf')

            reader = SequentialReader()
            reader.open(
                StorageOptions(uri=self.input_path.get(), storage_id='sqlite3'),
                ConverterOptions('', '')
            )
            reader.set_filter(StorageFilter(topics=selected_topics))

            writer = SequentialWriter()
            writer.open(
                StorageOptions(uri=self.output_path.get(), storage_id='sqlite3'),
                ConverterOptions('', '')
            )

            for topic_name in selected_topics:
                topic_type, _ = self.available_topics[topic_name]
                writer.create_topic(TopicMetadata(
                    name=topic_name,
                    type=topic_type,
                    serialization_format='cdr'
                ))

            while reader.has_next():
                topic, data, timestamp = reader.read_next()
                if start_ns <= timestamp <= end_ns:
                    writer.write(topic, data, timestamp)

            messagebox.showinfo("Success", "Bag processed successfully!")

        except Exception as e:
            messagebox.showerror("Error", f"Processing failed: {str(e)}")

    def validate_inputs(self):
        if not self.input_path.get():
            messagebox.showerror("Error", "Please select an input bag")
            return False
        if not self.output_path.get():
            messagebox.showerror("Error", "Please select an output directory")
            return False
        return True

    def get_time_offsets(self):
        try:
            start = float(self.start_time.get()) if self.start_time.get() else 0.0
            end = float(self.end_time.get()) if self.end_time.get() else float('inf')
            if start < 0 or end < start:
                raise ValueError("Invalid time range")
            return start, end
        except ValueError:
            messagebox.showerror("Error", "Invalid time values")
            raise

    def get_min_timestamp(self):
        bag_dir = self.input_path.get()
        db_file = self.find_sqlite_file(bag_dir)
        conn = sqlite3.connect(os.path.join(bag_dir, db_file))
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(timestamp) FROM messages")
        min_ts = cursor.fetchone()[0]
        conn.close()
        return min_ts

if __name__ == "__main__":
    root = tk.Tk()
    app = ROS2BagFilterApp(root)
    root.mainloop()