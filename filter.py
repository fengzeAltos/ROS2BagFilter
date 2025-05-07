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
import threading
from queue import Queue

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
        self.duration = 0.0
        self.min_ts = 0
        self.max_ts = 0
        self.progress_queue = Queue()
        self.processing = False
        
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

        # Time range slider
        ttk.Label(self.root, text="Time Range:").grid(row=3, column=0, sticky="w")
        self.time_slider_frame = ttk.Frame(self.root)
        self.time_slider_frame.grid(row=3, column=1, columnspan=2, sticky="ew", padx=5, pady=5)

        # Start time controls
        self.start_time_label = ttk.Label(self.time_slider_frame, text="Start Timestamp (ns): ")
        self.start_time_label.pack(anchor=tk.W)
        
        self.start_slider = ttk.Scale(
            self.time_slider_frame,
            from_=0,
            to=100,
            orient=tk.HORIZONTAL,
            command=self.update_start_time
        )
        self.start_slider.pack(fill=tk.X, expand=True)

        # End time controls
        self.end_time_label = ttk.Label(self.time_slider_frame, text="End Timestamp (ns): ")
        self.end_time_label.pack(anchor=tk.W)
        
        self.end_slider = ttk.Scale(
            self.time_slider_frame,
            from_=0,
            to=100,
            orient=tk.HORIZONTAL,
            command=self.update_end_time
        )
        self.end_slider.pack(fill=tk.X, expand=True)

        # Time entry fields
        self.time_entry_frame = ttk.Frame(self.root)
        self.time_entry_frame.grid(row=4, column=1, sticky="ew", padx=5, pady=5)
        
        ttk.Label(self.time_entry_frame, text="Start (s):").pack(side=tk.LEFT)
        self.start_time_entry = ttk.Entry(self.time_entry_frame, width=10)
        self.start_time_entry.pack(side=tk.LEFT, padx=5)
        self.start_time_entry.bind("<KeyRelease>", self.validate_start_time)
        
        ttk.Label(self.time_entry_frame, text="End (s):").pack(side=tk.LEFT, padx=(10,0))
        self.end_time_entry = ttk.Entry(self.time_entry_frame, width=10)
        self.end_time_entry.pack(side=tk.LEFT, padx=5)
        self.end_time_entry.bind("<KeyRelease>", self.validate_end_time)

        # Progress bar
        self.progress_frame = ttk.Frame(self.root)
        self.progress_frame.grid(row=5, column=1, sticky="ew", pady=10)
        
        self.progress_label = ttk.Label(self.progress_frame, text="Progress:")
        self.progress_label.pack(side=tk.LEFT, padx=5)
        
        self.progress_bar = ttk.Progressbar(
            self.progress_frame,
            orient=tk.HORIZONTAL,
            length=300,
            mode='determinate'
        )
        self.progress_bar.pack(side=tk.LEFT, expand=True, fill=tk.X)
        
        self.percentage_label = ttk.Label(self.progress_frame, text="0%")
        self.percentage_label.pack(side=tk.LEFT, padx=5)

        # Process button
        self.process_btn = ttk.Button(self.root, text="Process Bag", command=self.start_processing)
        self.process_btn.grid(row=6, column=1, pady=10)

    def grid_config(self):
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(2, weight=1)

    def setup_defaults(self):
        self.start_time_entry.insert(0, "0.0")
        self.end_time_entry.insert(0, "0.0")

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
            
            # Get topics and message counts
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.name, t.type, COUNT(m.topic_id)
                FROM topics t
                LEFT JOIN messages m ON t.id = m.topic_id
                GROUP BY t.id
            """
            )
            
            self.available_topics = {}
            self.topic_names = []
            self.topic_list.delete(0, tk.END)
            
            for name, type_, count in cursor.fetchall():
                display_text = f"{name} ({type_}) [{count} messages]"
                self.topic_list.insert(tk.END, display_text)
                self.topic_names.append(name)
                self.available_topics[name] = (type_, count)
            
            # Get time range
            self.min_ts = self.get_min_timestamp(bag_dir, db_file)
            self.max_ts = self.get_max_timestamp(bag_dir, db_file)
            self.duration = (self.max_ts - self.min_ts) / 1e9
            
            # Configure sliders
            self.start_slider.config(to=self.duration)
            self.end_slider.config(to=self.duration)
            self.start_slider.set(0)
            self.end_slider.set(self.duration)
            self.start_time_entry.delete(0, tk.END)
            self.start_time_entry.insert(0, "0.0")
            self.end_time_entry.delete(0, tk.END)
            self.end_time_entry.insert(0, f"{self.duration:.2f}")
            
            self.update_time_labels()
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

    def get_min_timestamp(self, bag_dir, db_file):
        conn = sqlite3.connect(os.path.join(bag_dir, db_file))
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(timestamp) FROM messages")
        min_ts = cursor.fetchone()[0]
        conn.close()
        return min_ts

    def get_max_timestamp(self, bag_dir, db_file):
        conn = sqlite3.connect(os.path.join(bag_dir, db_file))
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM messages")
        max_ts = cursor.fetchone()[0]
        conn.close()
        return max_ts

    def update_time_labels(self):
        """Update timestamp labels with raw nanoseconds"""
        try:
            start_sec = float(self.start_time_entry.get())
            end_sec = float(self.end_time_entry.get())
            
            start_ns = int(self.min_ts + start_sec * 1e9)
            end_ns = int(self.min_ts + end_sec * 1e9)
            
            self.start_time_label.config(text=f"Start Timestamp (ns): {start_ns}")
            self.end_time_label.config(text=f"End Timestamp (ns): {end_ns}")
        except:
            pass

    def update_start_time(self, value):
        try:
            val = float(value)
            if val > float(self.end_time_entry.get()):
                self.end_time_entry.delete(0, tk.END)
                self.end_time_entry.insert(0, f"{val:.2f}")
                self.end_slider.set(val)
            self.start_time_entry.delete(0, tk.END)
            self.start_time_entry.insert(0, f"{val:.2f}")
            self.update_time_labels()
        except ValueError:
            pass

    def update_end_time(self, value):
        try:
            val = float(value)
            if val < float(self.start_time_entry.get()):
                self.start_time_entry.delete(0, tk.END)
                self.start_time_entry.insert(0, f"{val:.2f}")
                self.start_slider.set(val)
            self.end_time_entry.delete(0, tk.END)
            self.end_time_entry.insert(0, f"{val:.2f}")
            self.update_time_labels()
        except ValueError:
            pass

    def validate_start_time(self, event):
        try:
            val = float(self.start_time_entry.get())
            if val < 0:
                val = 0.0
            elif val > self.duration:
                val = self.duration
            self.start_slider.set(val)
            if val > float(self.end_time_entry.get()):
                self.end_slider.set(val)
                self.end_time_entry.delete(0, tk.END)
                self.end_time_entry.insert(0, f"{val:.2f}")
            self.update_time_labels()
        except ValueError:
            pass

    def validate_end_time(self, event):
        try:
            val = float(self.end_time_entry.get())
            if val < 0:
                val = 0.0
            elif val > self.duration:
                val = self.duration
            self.end_slider.set(val)
            if val < float(self.start_time_entry.get()):
                self.start_slider.set(val)
                self.start_time_entry.delete(0, tk.END)
               	self.start_time_entry.insert(0, f"{val:.2f}")
            self.update_time_labels()
        except ValueError:
            pass

    def start_processing(self):
        if not self.validate_inputs():
            return
        self.processing = True
        self.process_btn.config(state=tk.DISABLED)
        self.progress_bar['value'] = 0
        self.percentage_label.config(text="0%")
        
        selected_indices = self.topic_list.curselection()
        selected_topics = [self.topic_names[i] for i in selected_indices]
        start_time = float(self.start_time_entry.get())
        end_time = float(self.end_time_entry.get())

        threading.Thread(
            target=self.process_bag,
            args=(selected_topics, start_time, end_time),
            daemon=True
        ).start()

        # kick off the progress-poll loop once
        self.root.after(50, self.check_progress)

    def process_bag(self, selected_topics, start_time, end_time):
        try:
            start_ns = int(self.min_ts + start_time * 1e9)
            end_ns = int(self.min_ts + end_time * 1e9)
            total_duration = end_ns - start_ns

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

            # Create topics first
            for topic_name in selected_topics:
                topic_type, _ = self.available_topics[topic_name]
                writer.create_topic(TopicMetadata(
                    name=topic_name,
                    type=topic_type,
                    serialization_format='cdr'
                ))

            while reader.has_next() and self.processing:
                topic, data, timestamp = reader.read_next()
                
                # Update progress based on timestamp position
                if total_duration > 0:
                    progress = ((timestamp - start_ns) / total_duration) * 100
                    progress = max(0, min(progress, 100))
                    self.progress_queue.put(progress)
                else:
                    self.progress_queue.put(100.0)
                
                # Write message if within time range
                if start_ns <= timestamp <= end_ns:
                    writer.write(topic, data, timestamp)

            # ensure we get to 100%
            self.progress_queue.put(100.0)
            # signal success
            self.progress_queue.put("SUCCESS")
        except Exception as e:
            # reset progress
            self.progress_queue.put(0.0)
            # signal error with message
            self.progress_queue.put(("ERROR", str(e)))
        finally:
            self.processing = False

    def check_progress(self):
        while not self.progress_queue.empty():
            msg = self.progress_queue.get_nowait()
            if isinstance(msg, float):
                pct = max(0.0, min(100.0, msg))
                self.progress_bar['value'] = pct
                self.percentage_label.config(text=f"{pct:.1f}%")
            elif msg == "SUCCESS":
                messagebox.showinfo("Success", "Bag processed successfully!")
            elif isinstance(msg, tuple) and msg[0] == "ERROR":
                messagebox.showerror("Error", f"Processing failed: {msg[1]}")

        if self.processing:
            self.root.after(50, self.check_progress)
        else:
            self.process_btn.config(state=tk.NORMAL)

    def validate_inputs(self):
        if not self.input_path.get():
            messagebox.showerror("Error", "Please select an input bag")
            return False
        if not self.output_path.get():
            messagebox.showerror("Error", "Please select an output directory")
            return False
        try:
            selected_indices = self.topic_list.curselection()
            if not selected_indices:
                messagebox.showerror("Error", "No topics selected!")
                return False
            
            start = float(self.start_time_entry.get())
            end = float(self.end_time_entry.get())
            if start < 0 or end < start or end > self.duration:
                messagebox.showerror("Error", "Invalid time range")
                return False
        except ValueError:
            messagebox.showerror("Error", "Invalid time values")
            return False
        return True

if __name__ == "__main__":
    root = tk.Tk()
    app = ROS2BagFilterApp(root)
    root.mainloop()
