import os, subprocess, shutil, signal, docker
import sys, time, threading
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QDate,QProcess, QUrl, Qt, QTimer, QThread, pyqtSignal, pyqtSlot, QBasicTimer,pyqtSignal
from PyQt5.QtWidgets import QApplication,QDialog, QProgressBar, QDateEdit,QHBoxLayout,QLineEdit, QApplication, QMainWindow, QStackedWidget, QFileDialog,QWidget, QVBoxLayout, QGridLayout, QPushButton, QLabel, QListWidget, QListWidgetItem, QMessageBox
from PyQt5.QtWebEngineWidgets import *
from PyQt5.QtGui import QDesktopServices,QFont
#from send2trash import send2trash
from subprocess import check_output
import tkinter as tk
from tkinter import messagebox

#Custom widgets
class ProgressDialog(QDialog):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        #print("Called 1")
        self.setWindowTitle("Progress")
        self.progress_bar = QProgressBar()
        layout = QVBoxLayout()
        layout.addWidget(self.progress_bar)
        self.setLayout(layout)
        self.setWindowModality(Qt.ApplicationModal)
        self.setMaximumSize(300, 100)

    def set_progress(self, value):
        print("Entered set progress")
        self.progress_bar.setValue(value)

class dataFiles2(QThread):
    progressUpdate = pyqtSignal(int)
    def run(self):
        directory_path = r'.\hadoop_namenode'
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        else:
            print(f"*Ignore* Directory '{directory_path}' does not exist.")
        self.progressUpdate.emit(25)  # Update progress to 25%
        command2 = f'docker cp sparkcontainer:/app/datafiles hadoop_namenode/'
        output2 = subprocess.check_output(command2, shell=True)
        print(output2.decode())
        print("Transferring to volume")
        self.progressUpdate.emit(35)  # Update progress to 35%
        command3 = f'docker cp hadoop_namenode/ namenode:/tmp'
        output3 = subprocess.check_output(command3, shell=True)
        print(output3.decode())
        print("Transferring to namenode")
        self.progressUpdate.emit(50)  # Update progress to 50%
        command35 = 'docker exec -it namenode hdfs dfs -test -d /data'
        output35 = subprocess.run(command35, shell=True, capture_output=True, text=True)
        if output35.returncode == 0:
            # Remove all files in the /data directory in HDFS
            command36 = 'docker exec -it namenode hdfs dfs -rm /data/*'
            subprocess.run(command36, shell=True, check=True)
            self.progressUpdate.emit(70)  # Update progress to 70%
            # Remove the /data directory in HDFS
            command37 = 'docker exec -it namenode hdfs dfs -rmdir /data'
            subprocess.run(command37, shell=True, check=True)
            self.progressUpdate.emit(90)  # Update progress to 90%
        else:
            print('*Ignore*/data does not exist in HDFS')

        command4 = f'docker exec -it namenode hdfs dfs -put /tmp/hadoop_namenode /data'
        subprocess.check_output(command4, shell=True)

    def set_progress(self, value):
        #print("Entered set progress")
        self.progress_bar.setValue(value)

class ListBoxWidget(QListWidget):
    dropEventSignal = QtCore.pyqtSignal() # Custom signal
    def __init__(self, parent=None):   
        #print("Called 2")
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.resize(600, 300)
        container_id = "sparkcontainer"  # Replace with your container name or ID
        directory = r"/app/datafiles"
        command = f"docker exec {container_id} ls {directory}"
        try:
            output = check_output(command, shell=True).decode()
            files = output.split("\n")
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(directory, file)
                    item = QListWidgetItem(file_path)
                    item = item.text().replace('\\', '/')
                    self.addItem(item)
        except Exception as e:
            print(f"No files: {e}")

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls:
            event.accept()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.CopyAction)
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.CopyAction)
            event.accept()

            links = []
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    file_path = str(url.toLocalFile())
                    links.append(file_path)
                else:
                    file_path = str(url.toString())
                    links.append(file_path)

            # Check if the files are already present in the list
            existing_files = [self.item(index).text() for index in range(self.count())]
            new_files = [file_path for file_path in links if file_path not in existing_files]

            self.addItems(new_files)

            for file_path in new_files:
                file_extension = os.path.splitext(file_path)[1]

            #if file_extension == ".csv":
            #    print("Source file is a .csv file.")
            #else:
            #    print("Source file is not a .csv file.")

            process = QProcess(None)
            process.finished.connect(process.deleteLater)  # Clean up the process
            process.setProcessChannelMode(QProcess.MergedChannels)  # Merge stdout and stderr

            command1 = "docker cp " + file_path + r" sparkcontainer:/app/datafiles"
            #print("*Ignore*",command1)
            subprocess.run(command1, shell=True, check=True)

        else:
            event.ignore()
        
        self.dropEventSignal.emit() # Emit the custom signal

    def fileDel (self, filePath):
        item = self.listbox.findItems(filePath, Qt.MatchExactly)
        if item:
            self.listbox.takeItem(self.listbox.row(item[0]))

class ProcessThread(QThread):
    

    new_output = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, command):
        super().__init__()
        self.command = command
        #print("Called 3")

    def run(self):
        try:
            process = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True  # Set universal_newlines to True to receive decoded output directly
            )
            stdout, _ = process.communicate()  # Use communicate to read both stdout and stderr
            self.new_output.emit(stdout)  # Emit the output signal with the received output
        except Exception as e:
            self.error_occurred.emit(str(e))

class DataCleaningThread(QThread):
    

    progress_updated = pyqtSignal(int)

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        #print("Called 4s")

    def run(self):
        command = self.filename
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, b''):
            output = line.decode().strip()
            if output.startswith("Progress:"):
                progress = int(output.split(":")[1].strip().replace("%", ""))
                self.progress_updated.emit(progress)
        process.stdout.close()
        process.wait()
#====================================ENTITIES=======================================

class startApp:
    def startContainer(self,stackedWidget):
        self.stackedWidget = stackedWidget
        #print("Start container")
        loadingpage = loadingPage(self.stackedWidget)
        self.stackedWidget.addWidget(loadingpage)
        #print("Going to starting page")
        self.stackedWidget.setCurrentWidget(loadingpage)
        self.stackedWidget.show()

    def firstSetup(self, stackedWidget):
        self.stackedWidget = stackedWidget
        setuppage = FirstSetupPage(self.stackedWidget)
        self.stackedWidget.addWidget(setuppage)
        #print("Going to setup page")
        self.stackedWidget.setCurrentWidget(setuppage)
        self.stackedWidget.show() 

    def aboutUs(self):
        url = QUrl("https://uowfyps120.wixsite.com/fyp23s120")
        QDesktopServices.openUrl(url)
    

class containerE:
    def refreshContainer(self, containerList):
        self.containerList = containerList
        self.docker_client = docker.from_env()
        self.containerList.clear()
        for c in self.docker_client.containers.list(all=True):
            item = QListWidgetItem()
            item.setData(1, c.id)
            item.setData(2, c.status)
            item.setText(f"{c.name} - {c.status}")
            self.containerList.addItem(item)
    
    def checkContainers(self):
        self.docker_client = docker.from_env()
        for c in self.docker_client.containers.list(all=True):
            if c.status == "exited":
                confirm = QMessageBox.question(self, 'Containers', 'Containers not running. \nDo you want to continue to Main Page without starting the containers?',
                                                QMessageBox.Yes | QMessageBox.No)
                if confirm == QMessageBox.Yes: 
                    return True
                else:
                    return False
            else:
                return True

    
    def containersPresent(self):
        self.docker_client = docker.from_env()
        containerList = []
        containerReal = ['kafcontainer', 'sparkcontainer', 'datanode', 'namenode', 'historyserver', 'resourcemanager', 'viscontainer', 'zoocontainer', 'nodemanager', 'viscontainer2']
        containerReal.sort()
        containers = self.docker_client.containers.list(all = True)
        for container in containers:
            containerList.append(container.name)
        containerList.sort()
        #print(containerList)
        if containerReal == containerList:
            #print("Is inside")
            for container in containers:
                if container.status == "exited":
                     return False
                else:
                    return True
        else:
            #print("Not inside")
            return False
        
    def containersPresent2(self):
        self.docker_client = docker.from_env()
        containerList = []
        containerReal = ['kafcontainer', 'sparkcontainer', 'datanode', 'namenode', 'historyserver', 'resourcemanager', 'viscontainer', 'zoocontainer', 'nodemanager', 'viscontainer2']
        containerReal.sort()
        containers = self.docker_client.containers.list(all = True)
        for container in containers:
            containerList.append(container.name)
        containerList.sort()
        #print(containerList)
        if containerReal == containerList:
            #print("Is inside")
            for container in containers:
                if container.status == "exited":
                     return True
        else:
            print("Not inside")
            return False


#For Historical data page
class dataFiles(QThread):
    
    def refreshFiles(self,listview):
        #print("Listing files...")
        #print("Did it come here? Yes")
        self.listview = listview
        self.listview.clear()  # Clear the listbox view
        container_id = "sparkcontainer" 
        directory = r"/app/datafiles"
        command = f"docker exec {container_id} ls {directory}"
        try:
            output = check_output(command, shell=True).decode()
            files = output.split("\n")
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(directory, file)
                    item = QListWidgetItem(file_path)
                    item = item.text().replace('\\', '/')
                    self.listview.addItem(item)  # Add items back to the listbox view
        except Exception as e:
            print(f"Error listing files: {e}")

    def processData(self,listview, progressBar, cleaningThread):
        self.listview = listview
        self.progressBar = progressBar
        self.progressBar.show()
        try:
            directory_path = r'.\hadoop_namenode'
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
            else:
                print(f"Directory '{directory_path}' does not exist.")

            
            #process data   
            item = self.listview.currentItem()
            full_path = item.text()

            # Extract filename from full path
            filename = os.path.basename(full_path)
            command5 = f'docker exec -it sparkcontainer spark-submit cleanHistoricalData.py hdfs://namenode:9000/data/' + filename

            if self.data_cleaning_thread is None:
                self.progressBar.show()
                self.data_cleaning_thread = DataCleaningThread(command5)
                self.data_cleaning_thread.progress_updated.connect(self.progressBar.setValue)
                self.data_cleaning_thread.finished.connect(lambda: data_cleaning_finished(self.data_cleaning_thread,self.progressBar))
                self.data_cleaning_thread.start()
                
            def data_cleaning_finished(data_cleaning_thread, progress_bar):
                data_cleaning_thread.quit()
                data_cleaning_thread.wait()
                data_cleaning_thread.deleteLater()
                progress_bar.hide()
                messagebox.showinfo('Finished', 'Processing complete!')
        except Exception as e:
            # Show a message box with the error message if an exception occurs
            messagebox.showerror('Error', str(e))

    
        

    def delFile(self, listview, x , text):
        self.listview = listview
        self.x = x
        self.text = text

        file_path = x
        # Show a confirmation dialog before deleting the file
        confirm = QMessageBox.question(self, 'Delete File', 'Are you sure you want to delete this file?',
                                        QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.Yes:
            selected_item = self.listview.currentItem()
            if selected_item is not None:
                file_path = selected_item.text()
                #print(file_path)
                if file_path:
                    try:
                        self.listview.takeItem(self.listview.row(selected_item))  # Remove the item from the QListWidget
                        container_id = "sparkcontainer"
                        command = f"docker exec {container_id} rm {file_path}"  # Delete the file from the container
                        check_output(command, shell=True)
                    except Exception as e:
                        print(f"Error deleting file: {e}")
        self.listview.clearSelection()

    def exFile(self, listview, x , text):
        self.listview = listview
        self.x = x
        self.text = text

        file_path = x
        # Show a confirmation dialog before deleting the file
        confirm = QMessageBox.question(self, 'Export File', 'Are you sure you want to export this file?',
                                        QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.Yes:
            selected_item = self.listview.currentItem()
            if selected_item is not None:
                file_path = selected_item.text()
                if file_path:
                    try:
                        container_id = "sparkcontainer"

                        directory = QFileDialog.getExistingDirectory(self, "Select directory")
                        if directory:
                            # Use the selected directory for the command
                            command1 = f'docker cp {container_id}:{file_path} {directory}'
                            output = subprocess.check_output(command1, shell=True)
                            print(output.decode())

                            self.listview.takeItem(self.listview.row(selected_item))  # Remove the item from the QListWidget
                            
                            command = f"docker exec {container_id} rm {file_path}"  # Delete the file from the container
                            check_output(command, shell=True)
                        else:
                            print("No destination selected")
                    except Exception as e:
                        print(f"Error deleting file: {e}")
        self.listview.clearSelection()

    def getHistoricalData(self, stackedWidget, selectedCountry, startDate, endDate, listview):
        self.stackedWidget = stackedWidget
        self.selectedcountry = selectedCountry
        self.startDateCal = startDate
        self.endDateCal = endDate
        self.listview = listview
        items = [self.selectedcountry.item(i).text() for i in range(self.selectedcountry.count())]
        items_str = ' '.join(items)
        start_date = self.startDateCal.date().toPyDate()
        end_date = self.endDateCal.date().toPyDate()

        try:
            if start_date > end_date:
                raise ValueError("Start date cannot be later than end date.")

            if not items_str:
                raise ValueError("Country Selection is empty.")
            message = f'Are you sure you want to retrieve data from these countries and date? \nCountries: {items_str}\nDate: {start_date} to {end_date}'        
            confirm = QMessageBox.question(self, 'Retrieve Data', message ,
                                            QMessageBox.Yes | QMessageBox.No)
        
            if confirm == QMessageBox.Yes:
                confirm2 = QMessageBox(self)
                confirm2.setIcon(QMessageBox.Information)
                confirm2.setText("Retrieving...")
                confirm2.setWindowModality(Qt.ApplicationModal)
                confirm2.show()

                # Process events to ensure that the message box is displayed
                QApplication.processEvents()

                # Call the function
                dataFiles.getHist(self, self.listview)

                # Hide the message box and enable the Ok button
                confirm2.setStandardButtons(QMessageBox.Ok)
                # Show the message box again and start the event loop
                confirm2.exec_()
                
                if confirm2.clickedButton() == confirm2.button(QMessageBox.Ok):
                    self.selectedcountry.clear()
                    print("Going to Historical Data page")
                    self.stackedWidget.setCurrentIndex(2)
        except ValueError as e:
            QMessageBox.warning(self, 'Error', str(e))

    def getHist(self, listview):

        items = [self.selectedcountry.item(i).text() for i in range(self.selectedcountry.count())]
        locations = ' '.join(items)
        Date_is = self.startDateCal.date().toPyDate()
        Date_end = self.endDateCal.date().toPyDate()
        self.listview = listview

        directory_path = r'.\hadoop_namenode'
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        else:
            print(f"Directory '{directory_path}' does not exist.")
        command1 = f'docker exec -it sparkcontainer python3 main.py {locations} {Date_is} {Date_end}'
        #command1 = f'docker cp C:\\Users\\Owner\\Desktop\\FYP\\Test\\Changes\\FypApp\\Spark\\data-clean-realtime.py sparkcontainer:/app'
        output1 = subprocess.check_output(command1, shell=True)
        print(output1.decode())

        dataFiles.trxHDFS(self, self.listview)

    

    def trxHDFS(self, listview):
        self.listview = listview
        directory_path = r'.\hadoop_namenode'
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        else:
            print(f"*Yes Ignore* Directory '{directory_path}' does not exist.")
        
        command2 = f'docker cp sparkcontainer:/app/datafiles hadoop_namenode/'
        output2 = subprocess.check_output(command2, shell=True)
        print(output2.decode())
        #print("Tranferring to volume")
        
        command3 = f'docker cp hadoop_namenode/ namenode:/tmp'
        output3 = subprocess.check_output(command3, shell=True)
        print(output3.decode())
        #print("transferring to namenode")
        
       
        command35 = 'docker exec -it namenode hdfs dfs -test -d /data'
        output35 = subprocess.run(command35, shell=True, capture_output=True, text=True)
        if output35.returncode == 0:
            # Remove all files in the /data directory in HDFS
            command36 = 'docker exec -it namenode hdfs dfs -rm /data/*'
            subprocess.run(command36, shell=True, check=True)
                            
            # Remove the /data directory in HDFS
            command37 = 'docker exec -it namenode hdfs dfs -rmdir /data'
            subprocess.run(command37, shell=True, check=True)
            
        else:
            print('*Ignore*/data does not exist in HDFS')


        command4 = f'docker exec -it namenode hdfs dfs -put /tmp/hadoop_namenode /data'
        output4 = subprocess.check_output(command4, shell=True)
        #print(output4.decode())
       #print("transferring to hdfs")
        
        dataFiles.refreshFiles(self, self.listview)

class streamLit:
    def analyseData(self):
        try:
            directory_path = r'.\hadoop_namenode'
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
            else:
                print(f"Directory '{directory_path}' does not exist.")
            command6 = f'docker cp sparkcontainer:/usr/local/output/ hadoop_namenode/'
            check_output(command6, shell=True)

            # Check if /usr/local/hadoop_namenode exists in viscontainer
            command = 'docker exec -it viscontainer test -d /usr/local/hadoop_namenode && echo "Exists"'
            output = subprocess.run(command, shell=True, capture_output=True, text=True)
            
            if "Exists" in output.stdout:
                # Remove all files in /usr/local/hadoop_namenode
                command = 'docker exec -it viscontainer rm -rf /usr/local/hadoop_namenode/*'
                subprocess.run(command, shell=True, check=True)
                
                # Remove the /usr/local/hadoop_namenode directory
                command = 'docker exec -it viscontainer rm -rf /usr/local/hadoop_namenode'
                subprocess.run(command, shell=True, check=True)
            else:
                print('/usr/local/hadoop_namenode does not exist in viscontainer')


            command7 = f'docker cp hadoop_namenode/ viscontainer:/usr/local/'
            check_output(command7, shell=True)
        except subprocess.CalledProcessError as e:
            print(f'Error: {e.returncode}, Output: {e.output.decode()}')
        
        url = QUrl("http://localhost:8501")  # URL to open in the web browser
        QDesktopServices.openUrl(url)

#For RT Stream
class stream:
    def startStream(self, streamLabel, selectedcountry):
        self.streamLabel = streamLabel
        self.selectedcountry = selectedcountry
        items = [self.selectedcountry.item(i).text() for i in range(self.selectedcountry.count())]
        locations = ' '.join(items)

        try:
            if locations:
                message = f'Are you sure you want to start streaming from these countries? \nCountries: {locations}'     
                confirm = QMessageBox.question(self, 'Start Stream', message ,
                                                QMessageBox.Yes | QMessageBox.No)
                if confirm == QMessageBox.Yes:
                    command1 = f"docker exec -d sparkcontainer python3 /app/data/dataRT.py {locations}"
                    subprocess.run(command1, shell=True, check=True)
                    print("Stream1 started successfully.")

                    command2 = f'docker run -it --rm ubunimage python3 /app/bin/sendStream.py -h'
                    output2 = subprocess.check_output(command2, shell=True)
                    print(output2.decode())
                    print("command 2 ran")

                    command3 = f'docker exec -d sparkcontainer python3 /app/bin/processStream.py my-stream'
                    self.process53 = subprocess.Popen(command3, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    print("Stream2 started successfully.")

                    command4 = f'docker exec -it sparkcontainer python3 /app/bin/sendStream.py /app/data.csv my-stream'
                    self.process_thread = ProcessThread(command4)
                    self.process_thread.start()
                    print("Stream3 started successfully.")

                    popup = QMessageBox(self)
                    popup.setWindowTitle("Stream")
                    popup.setIcon(QMessageBox.Information)
                    popup.setText("Stream starting...")
                    popup.setWindowFlag(Qt.WindowStaysOnTopHint)
                    popup.setStandardButtons(QMessageBox.NoButton)

                    # Create timer to show "OK" button after 30 seconds
                    timer = QTimer(self)
                    timer.setSingleShot(True)
                    timer.timeout.connect(lambda: popup.setStandardButtons(QMessageBox.Ok))
                    timer.start(20000)

                    popup.exec_()

                    self.streamLabel.setText("Stream status: Stream is running.")
                    self.selectedcountry.clear()
            else:
                raise ValueError("Please select a country")
            
        except ValueError as e:
            QMessageBox.warning(self,'Warning', str(e), QMessageBox.Ok)
            print(str(e))
    
    def stopStream(self, streamLabel):
        self.streamLabel = streamLabel
        commands = [
        "docker exec sparkcontainer pgrep -f /app/data/dataRT.py",
        "docker exec sparkcontainer pgrep -f /app/bin/processStream.py",
        "docker exec sparkcontainer pgrep -f /app/bin/sendStream.py"
        ]
        try:
            if self.streamLabel == "Stream status: Stream has stopped.":
                if self.streamLabel == "Stream status: Not streaming.":
                    raise ValueError("Stream is not running")
            else:
                message = f'Are you sure you want to stop the stream?'     
                confirm = QMessageBox.question(self, 'Stop Stream', message ,
                                                QMessageBox.Yes | QMessageBox.No)
                if confirm == QMessageBox.Yes:
                    for command in commands:
                        try:
                            output = subprocess.check_output(command, shell=True)
                            pids = output.decode().strip().split("\n")
                            for pid in pids:
                                try:
                                    subprocess.check_output(f"docker exec sparkcontainer kill {pid}", shell=True)
                                    print(f"Process with PID {pid} killed.")
                                except subprocess.CalledProcessError as e:
                                    print(f"Error killing process with PID {pid}: {e}")
                            self.streamLabel.setText("Stream status: Stream has stopped.")
                            print("Stream stopped successfully.")
                        except subprocess.CalledProcessError as e:
                            print(f"Error getting process ID: {e}")
        except ValueError as e:
            QMessageBox.warning(self,'Warning', str(e), QMessageBox.Ok)
        
    def viewRT(self):
        message = f'Proceed to analyse RT Data?'     
        confirm = QMessageBox.question(self, 'Analyse RT', message ,
                                        QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.Yes:
            try:
                directory_path = r'.\hadoop_namenode'
                if os.path.exists(directory_path):
                    shutil.rmtree(directory_path)
                else:
                    print(f"Directory '{directory_path}' does not exist.")
                command6 = f'docker cp sparkcontainer:/usr/local/output2 hadoop_namenode/'
                check_output(command6, shell=True)
                command7 = f'docker cp hadoop_namenode/ viscontainer2:/usr/local/'
                check_output(command7, shell=True)

                url = QUrl("http://localhost:8502")  # URL to open in the web browser
                QDesktopServices.openUrl(url)
        
            except subprocess.CalledProcessError as e:
                print(f'Error: {e.returncode}, Output: {e.output.decode()}')

    def checkStream(self):
        if self.streamLabel == "Stream status: Stream is running.":
            return True
        else:
            return False
            

#For user to view and select countries        
class countries:
    def viewCountries(self, availCountry):
        self.availCountry = availCountry
        try:
            with open('countries.txt', 'r') as f:
                countries = sorted(f.read().splitlines())
                self.availCountry.addItems(countries)
                #self.saveCountries()
        except FileNotFoundError:
            print('File not found.')

    def saveCountries(self, availCountry):
        self.availCountry = availCountry
        with open('countries.txt', 'w') as f:
            for i in range(self.availCountry.count()):
                f.write(self.availCountry.item(i).text() + '\n')

    def addCountry(self, availCountry, countryEdit):
        self.country_edit = countryEdit
        self.availCountry = availCountry
        country_name = self.country_edit.text().strip()
        try:
            if country_name:
                try:
                    for i in range(self.availCountry.count()):
                        if country_name == self.availCountry.item(i).text():
                            raise ValueError('Country already exists.')
                    self.availCountry.addItem(country_name)
                    countries.saveCountries(self, self.availCountry)
                    self.country_edit.clear()
                    # self.selectList.append(country_name)
                    # self.selectedcountry.setText('Selected countries: ' + ', '.join(self.selectList)
                    self.availCountry.clearSelection()
                except ValueError as e:
                    QMessageBox.warning(self, 'Warning', str(e), QMessageBox.Ok)
                    print(str(e))
                    self.country_edit.clear()           
            else:
                self.country_edit.clear()
                raise ValueError('Please enter a country')
        except ValueError as e:
            QMessageBox.warning(self, 'Warning', str(e), QMessageBox.Ok)
            print(str(e))

            
    def delCountry(self, availCountry):
        self.availCountry = availCountry
        country_name = self.availCountry.selectedItems()
        selected_countries = [item.text() for item in self.availCountry.selectedItems()]
        if country_name:
            confirm = QMessageBox.question(self, 'Delete Country', 'Are you sure you want to delete ' + selected_countries[0] + ' ?',
                                                QMessageBox.Yes | QMessageBox.No)
            if confirm == QMessageBox.Yes:
                country_name = self.availCountry.currentItem()
                self.availCountry.takeItem(self.availCountry.row(country_name))
                self.availCountry.clearSelection()
                countries.saveCountries(self,self.availCountry)

        else:
            QMessageBox.warning(self, 'Warning', 'No country selected.', QMessageBox.Ok)
    
    def selCountry(self, availCountry, selectCountry, countryEdit):
        self.availCountry = availCountry
        self.country_edit = countryEdit
        self.selectedcountry = selectCountry
        country_name = [item.text() for item in self.availCountry.selectedItems()]
        try:
            # Check if a country has been selected
            if not country_name:
                raise ValueError("Please select a country.")

            # Check if the selected country has already been added to the list
            if country_name[0] in [self.selectedcountry.item(i).text() for i in range(self.selectedcountry.count())]:
                raise ValueError("Country already selected.")

            # Add the selected country to the list
            self.selectedcountry.addItem(country_name[0])
            self.availCountry.clearSelection()
            self.country_edit.clear()

        except ValueError as e:
            QMessageBox.warning(self, 'Warning', str(e), QMessageBox.Ok)
            self.country_edit.clear()
        
    def removeSelCountry(self, selectCountry):
        self.selectedcountry = selectCountry
        selected_items = self.selectedcountry.selectedItems()
        try:
            # Check if a country has been selected
            if not selected_items:
                raise ValueError("Please select a country.")

            for item in selected_items:
                self.selectedcountry.takeItem(self.selectedcountry.row(item))
                self.selectedcountry.clearSelection()

        except ValueError as e:
            QMessageBox.warning(self, 'Warning', str(e), QMessageBox.Ok)
            self.country_edit.clear()

class interface:
    def goTo0(self, stackwidget):
        self.stackedwidget = stackwidget
        #print("back button pressed")
        self.stackedwidget.setCurrentIndex(0)
    
    def goTo1(self, stackwidget):
        self.stackedwidget = stackwidget
        #print("back button pressed")
        self.stackedwidget.setCurrentIndex(1)

    def goTo2(self, stackwidget):
        self.stackedwidget = stackwidget
        #print("back button pressed")
        self.stackedwidget.setCurrentIndex(2)

#====================================CONTROLLERS=======================================
class startPageController:
    def enterPage(self, stackedWidget):
        self.stackedWidget = stackedWidget
        self.containers = containerE.containersPresent2(self)
        if self.containers == True:
            startApp.startContainer(self, self.stackedWidget)
        else:
            startApp.firstSetup(self,self.stackedWidget)

class containerStatusController:
    def containerStatusC(self, containerList):
        containerE.refreshContainer(self,containerList)

class refreshHistController:
    def refreshHistC(self,listview):
        self.listview = listview
        dataFiles.refreshFiles(self,self.listview)

class processHistController:
    def processHistC(self, listview, processStatus):
        self.processStatus = processStatus
        self.listview = listview
        self.data_cleaning_thread = None
        dataFiles.processData(self, self.listview, processStatus, self.data_cleaning_thread)

class viewHistController:
    def viewHistC(self):
        streamLit.analyseData(self)

class delExFileController:
    def delExFileC(self, listview, x, text):
        try:
            if x:
                if text == 'Delete':
                    dataFiles.delFile(self, listview, x,text)
                
                elif text == 'Export':
                    dataFiles.exFile(self, listview,x , text)
            else:
                raise ValueError("No file selected")
        except ValueError as e:
            QMessageBox.warning(self, 'Error', str(e))

class realTimeStreamController:
    def realTimeStreamC(self, streamLabel, selectedcountry):
        self.streamLabel = streamLabel
        self.selectedcountry = selectedcountry
        stream.startStream(self, self.streamLabel, self.selectedcountry)

class killStreamController:
    def killStreamC(self, streamLabel):
        self.streamLabel = streamLabel
        stream.stopStream(self, self.streamLabel)

class loadCountriesController:
    def loadCountriesC(self, availCountry):
        self.availCountry = availCountry
        countries.viewCountries(self, self.availCountry)
        
class addCountryController:
    def addCountryC(self,availCountry, countryEdit):
        self.availCountry = availCountry
        self.countryEdit = countryEdit
        countries.addCountry(self, self.availCountry, self.countryEdit)

class delCountryController:
    def delCountryC(self, availCountry):
        self.availCountry = availCountry
        countries.delCountry(self, self.availCountry)

class addSelController:
    def addSelC(self, availCountry, selectCountry, countryEdit):
        self.availCountry = availCountry
        self.countryEdit = countryEdit
        self.selectCountry = selectCountry
        countries.selCountry(self, self.availCountry, self.selectCountry, self.countryEdit)

class removeSelController:
    def removeSelC(self,selectCountry):
        self.selectCountry = selectCountry
        countries.removeSelCountry(self,self.selectCountry)

class fetchDataController:
    def fetchDataC(self, stackedWidget, selectedCountry, startDate, endDate, listview):
        self.stackedWidget = stackedWidget
        self.selectedCountry = selectedCountry
        self.startDate = startDate
        self.endDate = endDate
        self.listview = listview
        dataFiles.getHistoricalData(self,self.stackedWidget, self.selectedCountry, self.startDate, self.endDate, self.listview)

class analyseRTController:
    def analyseRTC(self):
        stream.viewRT(self)

class backButtonController:
    def backButtonC(self,stackedwidget, page):
        if page == 0:
            interface.goTo0(self, stackedwidget)
        elif page == 1:
            interface.goTo1(self, stackedwidget)
        elif page == 2:
            interface.goTo2(self, stackedwidget)

class aboutUsController:
    def aboutUsC(self):
        startApp().aboutUs()
#====================================BOUNDARIES=======================================

#Boundary for Starting page UI (page0)
class Page0(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget
        font = QtGui.QFont()
        font.setPointSize(16)

        layout0 = QVBoxLayout()
        self.container_list = QListWidget()
        layout0.setAlignment(Qt.AlignCenter)

        self.pushButton01 = QPushButton("Start")
        self.pushButton01.setFixedSize(250, 90)
        self.pushButton01.setFont(font)
        self.pushButton01.setObjectName("pushButton01")

        self.pushButton02 = QPushButton("About")
        self.pushButton02.setFixedSize(250, 90)
        self.pushButton02.setFont(font)
        self.pushButton02.setObjectName("pushButton02")

        self.pushButton01.clicked.connect(self.startPage)
        self.pushButton02.clicked.connect(self.goAbout)

        layout0.addWidget(self.pushButton01)
        layout0.addWidget(self.pushButton02)

        self.setLayout(layout0)
        
    def startPage(self):
        startPageController.enterPage(self, self.stackedWidget)
    
    def goAbout(self):
        aboutUsController.aboutUsC(self)
    
    def closeEvent(self):
        super().closeEvent()

#Boundary for main page UI (Page 1)
class Page1(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget
        
        font = QtGui.QFont()
        font.setPointSize(16)
        #1------------------------------------------------------------------------------------------

        # layout and page1
        layout1 = QGridLayout()

        #go back
        self.pushButton11 = QtWidgets.QPushButton("Back")
        self.pushButton11.setFixedSize(100, 30)
        self.pushButton11.setFont(font)
        self.pushButton11.setObjectName("pushButton11")

        #run container button
        self.pushButton12 = QtWidgets.QPushButton("Historical")
        self.pushButton12.setFixedSize(250, 90)
        self.pushButton12.setFont(font)
        self.pushButton12.setObjectName("pushButton12")

        #get data button
        self.pushButton13 = QtWidgets.QPushButton("Real-Time")
        self.pushButton13.setFixedSize(250, 90)
        self.pushButton13.setFont(font)
        self.pushButton13.setObjectName("pushButton213")
    

        self.container_list = QListWidget()
        #self.refreshContainerList()

        self.label1 = QLabel("Container - Status")

        # initialize the list
        self.timer1 = QTimer()
        self.timer1.timeout.connect(lambda: self.containerStatus(self.container_list))
        self.timer1.start(1000)

        #add widgets to layout1
        layout1.addWidget(self.label1,0,0)
        layout1.addWidget(self.container_list, 1,0,5,4)
        layout1.addWidget(self.pushButton11,7,5)
        layout1.addWidget(self.pushButton12, 1, 5) # Add the button to the grid layout1 at row 0, column 1
        layout1.addWidget(self.pushButton13, 1, 6) # Add the button to the grid layout1 at row 0, column 2
        #layout1.addWidget(self.pushButton14, 1, 0) # Add the button to the grid layout1 at row 1, column 0


        # add button functionality for page 1
        self.pushButton11.clicked.connect(self.goBack)
        self.pushButton12.clicked.connect(self.analyseHist)
        self.pushButton13.clicked.connect(self.analyseRT)
        #self.container_list.itemSelectionChanged.connect(self.containerSelectionChanged)
        #add layout1 to page1
        self.setLayout(layout1)

    #Goes back to starting page
    def goBack(self):
        backButtonController.backButtonC(self, self.stackedWidget, 0)

    #Goes to RT page
    def analyseRT(self):
        #print("Going to view RT data page 3")
        self.stackedWidget.setCurrentIndex(3)
    
    def analyseHist(self):
        #goes to page 3 to view hist data
        #print("Going to view historical data")
        self.stackedWidget.setCurrentIndex(2)

    def containerStatus(self, containerList):
        containerStatusController.containerStatusC(self,containerList)

    def closeEvent(self):
        super().closeEvent()

#Boundary for Historical Data UI (Page 2)
class Page2(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.trx_hdfs_thread = None
        self.progress_dialog = None
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(14)

        #2-----------------------------------------------------------------------------------------

        # layout and page2 (Hist page)
        layout2 = QGridLayout()

        #configure 2nd page
        self.label1 = QLabel("Data files")
        self.label1.setFont(font)

        self.listview = ListBoxWidget(self)
        self.refreshHist()
        self.listview.dropEventSignal.connect(self.trx2HDFS) # Connect the signal to a slot
        self.pushButton22= QPushButton("Get Data")
        self.pushButton22.setFixedSize(100,50)
        self.pushButton22.setFont(font)
                                  
        self.pushButton23= QPushButton("Process")
        self.pushButton23.setFixedSize(100,50)
        self.pushButton23.setFont(font)

        self.pushButton24= QPushButton("Analyse")
        self.pushButton24.setFixedSize(100,50)
        self.pushButton24.setFont(font)

        self.pushButton25= QPushButton("Delete")
        self.pushButton25.setFixedSize(100,50)
        self.pushButton25.setFont(font)

        self.pushButton26= QPushButton("Export")
        self.pushButton26.setFixedSize(100,50)
        self.pushButton26.setFont(font)

        self.backButton27 = QPushButton('Back')
        self.pushButton28 = QPushButton('Refresh', self)
        self.pushButton28.setFixedSize(100,50)
        self.pushButton28.setFont(font)

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setGeometry(50, 50, 300, 30)
        self.progress_bar.hide()
        self.processStatus = QLabel("Process Status: ")

        layoutP = QHBoxLayout()
        layoutP.addWidget(self.processStatus)
        layoutP.addWidget(self.progress_bar)

        self.pushButton22.clicked.connect(self.getdData)
        #self.pushButton22.clicked.connect(lambda: print(self.getSelectedItem()))
        self.pushButton23.clicked.connect(self.processHist)
        self.pushButton24.clicked.connect(self.viewHist)
        self.pushButton25.clicked.connect(lambda: self.delExFile('Delete'))
        self.pushButton26.clicked.connect(lambda: self.delExFile('Export'))
        self.backButton27.clicked.connect(self.goBack)
        self.pushButton28.clicked.connect(self.refreshHist)

        layout2.addWidget(self.label1, 0, 0)
        layout2.addWidget(self.listview, 1, 0 , 6, 1) #row, columns, occupy no. rows, occupy no. colums
        layout2.addWidget(self.pushButton22, 1, 1) 
        layout2.addWidget(self.pushButton28, 1, 2)
        layout2.addWidget(self.pushButton23, 2, 1)
        layout2.addWidget(self.pushButton24, 2, 2)
        layout2.addWidget(self.pushButton25, 3, 1)
        layout2.addWidget(self.pushButton26, 3, 2)
        layout2.addWidget(self.backButton27, 8, 3)
        layout2.addLayout(layoutP, 8,0)
        
        self.setLayout(layout2)

        timer = QTimer()
        timer.timeout.connect(lambda: self.refreshHist(self))
        timer.start(500)
        self.stackedWidget.currentChanged.connect(self.refreshHist)


    def closeEvent(self):
        super().closeEvent()

    def goBack(self):
        backButtonController.backButtonC(self, self.stackedWidget, 1)

    def getdData(self):
        #print("Going to get Data in historical")
        self.stackedWidget.setCurrentIndex(4)
        self.refreshHist()

    def refreshHist(self):
        refreshHistController.refreshHistC(self, self.listview)

    def processHist(self):
        processHistController.processHistC(self, self.listview, self.progress_bar)
        self.refreshHist()

    #open up streamlit
    def viewHist(self):
        viewHistController.viewHistC(self)

    def delExFile(self, text):
        selected_items = self.listview.selectedItems()
        selected_item = ''
        for item in selected_items:
            selected_item = selected_item + item.text()
        delExFileController.delExFileC(self, self.listview, selected_item, text)

    def getListView(self):
        listView = self.listview
        return listView
    
    def trx2HDFS(self):
        #print("Trxhdfs 2 function")
        self.progress_dialog = ProgressDialog()
        self.progress_dialog.show()
        
        self.trx_hdfs_thread = dataFiles2()
        self.trx_hdfs_thread.progressUpdate.connect(self.progress_dialog.set_progress)
        
        self.trx_hdfs_thread.finished.connect(self.progress_dialog.close)
        self.trx_hdfs_thread.finished.connect(self.refreshHist)
        
        self.trx_hdfs_thread.finished.connect(self.trx_hdfs_thread.deleteLater)  # Clean up the thread
       
        self.trx_hdfs_thread.start()
        

              
#Boundary for Real Time Data UI (Page 3)
class Page3(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)
        #3------------------------------------------------------------------------------------------

        # layout and page3(RT page)
        layout3 = QGridLayout()
        #configure 3rd page
        
        #self.listview = ListBoxWidget(self)
        self.pushButton33= QPushButton("Start")
        self.pushButton34= QPushButton("Stop")
        #self.pushButton35= QPushButton("Process")
        self.pushButton36= QPushButton("Analyse")
        self.backButton37 = QPushButton('Back')
        #self.textEdit = QTextEdit(self)
        #self.textEdit.setReadOnly(True)

        self.label1 = QLabel("Countries available")
        self.availCountry = QListWidget()
        self.loadCountries()
        self.label2 = QLabel("Countries selected")
        self.selectedcountry = QListWidget()

        self.addSelButton = QPushButton('Add to selection')      
        
        self.removeSelBtn = QPushButton('Remove from selection')

        self.addCountryButton = QPushButton('Add Country')
        self.country_edit = QLineEdit()
        hbox = QHBoxLayout()
        hbox.addWidget(self.country_edit)
        hbox.addWidget(self.addCountryButton)

        self.delCountryButton = QPushButton("Delete Country")

        self.streamLabel = QLabel("Stream status: Not streaming.")
        self.processLabel = QLabel()
 
        self.pushButton33.clicked.connect(self.realTimeStream)
        self.pushButton34.clicked.connect(self.killStream)
        self.backButton37.clicked.connect(self.goBack)
        #self.pushButton35.clicked.connect(self.fetchRTData)
        self.pushButton36.clicked.connect(self.analyseRT)

        self.addSelButton.clicked.connect(self.addSelection)
        self.removeSelBtn.clicked.connect(self.removeSelection)
        #self.addCountryButton.clicked.connect(self.addCountry)
        #self.delCountryButton.clicked.connect(self.delCountry)

        #self.timer = QTimer()
        #self.timer.timeout.connect(self.updateOutput)

        #layout3.addWidget(self.textEdit, 0, 0 , 4, 1) #row, columns, occupy no. rows, occupy no. colums
        layout3.addWidget(self.label1, 0,0)
        layout3.addWidget(self.availCountry,1,0,4,1)
        layout3.addWidget(self.pushButton33, 1, 1)
        layout3.addWidget(self.pushButton34, 1, 2)
        #layout3.addLayout(hbox, 5, 0)
        layout3.addWidget(self.addSelButton, 2,1)
        #layout3.addWidget(self.delCountryButton,5,1)
        
        layout3.addWidget(self.label2, 7,0)
        layout3.addWidget(self.selectedcountry,8,0,4,1)
        #layout3.addWidget(self.pushButton35, 8, 1)
        layout3.addWidget(self.pushButton36, 8, 1)
        layout3.addWidget(self.removeSelBtn,9,1)

        layout3.addWidget(self.processLabel, 13,0)
        layout3.addWidget(self.streamLabel,14, 0)
        layout3.addWidget(self.backButton37, 14, 3)
        
        self.setLayout(layout3)

    def goBack(self):
        backButtonController.backButtonC(self, self.stackedWidget, 1)

    def realTimeStream(self):
        realTimeStreamController.realTimeStreamC(self, self.streamLabel,self.selectedcountry)

    def killStream(self):
        killStreamController.killStreamC(self, self.streamLabel)

    def loadCountries(self):
        self.availCountry.clear()
        loadCountriesController.loadCountriesC(self, self.availCountry)
        
    def addCountry(self):
        addCountryController.addCountryC(self, self.availCountry, self.country_edit)

    def delCountry(self):
        delCountryController.delCountryC(self, self.availCountry)
        
    def addSelection(self):
        addSelController.addSelC(self, self.availCountry, self.selectedcountry, self.country_edit)
        
    def removeSelection(self):
        removeSelController.removeSelC(self, self.selectedcountry)
    
    def fetchRTData(self):
        items = [self.selectedcountry.item(i).text() for i in range(self.selectedcountry.count())]
        items_str = ' '.join(items)
        try:
            if not items_str:
                raise ValueError("No countries selected")
            message = f'Are you sure you want to retrieve data from these countries? \nCountries: {items_str}'     
            confirm = QMessageBox.question(self, 'Retrieve Data', message ,
                                            QMessageBox.Yes | QMessageBox.No)
            if confirm == QMessageBox.Yes:
                print("ok")
        except ValueError as e:
            QMessageBox.warning(self, 'Error', str(e))

    def analyseRT(self):
        analyseRTController.analyseRTC(self)
        
    def closeEvent(self):
        super().closeEvent()

#Boundary for getData in Historical Data (page4)
class getData(Page3):
    def __init__(self,stackedWidget):
        super().__init__(stackedWidget)
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        while self.layout().count():
            item = self.layout().takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
            else:
                sublayout = item.layout()
                while sublayout.count():
                    subitem = sublayout.takeAt(0)
                    subwidget = subitem.widget()
                    if subwidget is not None:
                        subwidget.deleteLater()
                self.layout().removeItem(sublayout)

        self.pg2 = Page2(self.stackedWidget)
        self.listview = self.pg2.getListView()
        # Set the default date for startDateCal
        default_date = QDate(2023, 3, 1)  # May 1st, 2023
        # Set the default date for endDateCal
        #default_date = QDate.currentDate()  # Today's date

        # Set up the date edit widgets
        self.startDate = QLabel("Start date:")
        self.startDateCal = QDateEdit()
        self.startDateCal.setDate(default_date)

        self.endDate = QLabel("End date:")
        self.endDateCal = QDateEdit()
        self.endDateCal.setDate(self.startDateCal.date().addMonths(1))

        self.backButton37 = QPushButton('Back')

        self.label1 = QLabel("Countries available")
        self.availCountry = QListWidget()
        self.loadCountries()
        self.label2 = QLabel("Countries selected")
        self.selectedcountry = QListWidget()

        self.addSelButton = QPushButton('Add to selection')              
        self.removeSelBtn = QPushButton('Remove from selection')
        self.retreiveBtn = QPushButton('Retrieve Data')

        self.addCountryButton = QPushButton('Add Country')
        self.country_edit = QLineEdit()
        hbox = QHBoxLayout()
        hbox.addWidget(self.country_edit)
        hbox.addWidget(self.addCountryButton)
        self.delCountryButton = QPushButton("Delete Country")


        self.backButton37.clicked.connect(self.goBack)
        self.addSelButton.clicked.connect(self.addSelection)
        self.removeSelBtn.clicked.connect(self.removeSelection)
        #self.addCountryButton.clicked.connect(self.addCountry)
        #self.delCountryButton.clicked.connect(self.delCountry)
        self.retreiveBtn.clicked.connect(self.fetchData) 

        #layout3.addWidget(self.textEdit, 0, 0 , 4, 1) #row, columns, occupy no. rows, occupy no. colums
        self.layout().addWidget(self.label1, 0,0)
        self.layout().addWidget(self.availCountry,1,0,4,1)
        self.layout().addWidget(self.startDate, 1, 1)
        self.layout().addWidget(self.startDateCal, 1, 2)
        self.layout().addWidget(self.endDate, 2, 1)
        self.layout().addWidget(self.endDateCal, 2, 2)
        #self.layout().addLayout(hbox, 5, 0)
        self.layout().addWidget(self.addSelButton, 3,1)
        #self.layout().addWidget(self.delCountryButton,5,1)
        
        self.layout().addWidget(self.label2, 7,0)
        self.layout().addWidget(self.selectedcountry,8,0,4,1)
        #layout25.addWidget(self.pushButton35, 8, 1)
        #layout25.addWidget(self.pushButton36, 8, 2)
        self.layout().addWidget(self.removeSelBtn,8,1)
        self.layout().addWidget(self.retreiveBtn,9,1)

        #layout25.addWidget(self.processLabel, 13,0)
        #layout25.addWidget(self.streamLabel,14, 0)
        self.layout().addWidget(self.backButton37, 14, 3)

    def goBack(self):
        backButtonController.backButtonC(self, self.stackedWidget, 2)

    def fetchData(self):
        fetchDataController.fetchDataC(self, self.stackedWidget, self.selectedcountry, self.startDateCal, self.endDateCal, self.listview)
        
#Boundary for loading page (page5)
class loadingPage(QWidget):
    def __init__(self, stackedWidget):
        #print("Entering loading page")
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(12)

        layout6 = QVBoxLayout()

        self.labelStart = QLabel('Starting please wait about a minute...')
        self.labelStart.setAlignment(Qt.AlignCenter)  # set alignment to center
        font2 = QFont()
        font2.setPointSize(30)  # set font size to 30
        self.labelStart.setFont(font2)  # set font for the label

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setGeometry(0 ,0 ,500, 80)

        layout6.addWidget(self.labelStart)
        layout6.addWidget(self.progress_bar)

        self.timer2 = QTimer()

        self.runProcess()

        self.setLayout(layout6)

    def runProcess(self):
        # Create QProcess and command string
        self.process = QProcess()
        command = 'docker-compose -f ./docker-compose.yml up'

        # Set process channel mode and start
        self.process.setProcessChannelMode(QProcess.MergedChannels)
        self.process.start(command)

        # Connect process signals to slots
        self.process.readyRead.connect(self.readProcessOutput)
        self.process.finished.connect(self.processFinished)

        self.progress_bar.setRange(0, 0)
        self.progress_bar.setValue(0)
        self.progress_bar.show()

        # Start timer for 1 minute and connect to slot
        
        self.timer2.setSingleShot(True)
        self.timer2.timeout.connect(self.timerFinished)
        self.timer2.start(60000)

    def readProcessOutput(self):
        # Read output from process and update progress bar
        process_output = str(self.process.readAll(), 'utf-8')
        self.progress_bar.setValue(self.progress_bar.value() + 1)

    def processFinished(self):
        # Stop timer and hide progress bar
        self.timer2.stop()
        self.progress_bar.hide()
        self.progress_bar.setValue(0)
        current_widget_index = self.stackedWidget.currentIndex()
        current_widget = self.stackedWidget.widget(current_widget_index)
        self.stackedWidget.removeWidget(current_widget)
        #print("Processed finished, going to page 1")
        self.stackedWidget.setCurrentIndex(1)
        #print("Processed finished")

    def timerFinished(self):
        self.timer2.stop()
        self.progress_bar.hide()
        self.progress_bar.setValue(0)
        current_widget_index = self.stackedWidget.currentIndex()
        current_widget = self.stackedWidget.widget(current_widget_index)
        self.stackedWidget.removeWidget(current_widget)
        #print("Timer finished, going to page 1")
        self.stackedWidget.setCurrentIndex(1)
        # Do something when timer finished
        #print("Timer finished")

    def closeEvent(self):
        super().closeEvent()

class FirstSetupPage(QWidget):
    def __init__(self, stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        process = QProcess(None)
        command1 = r'docker-compose -f .\docker-compose.yml up'
        process.startDetached(command1)

        layout6 = QVBoxLayout()

        self.labelStart = QLabel('Setting up containers for initial setup...\n(About 8-10 minutes.)')
        self.labelStart.setAlignment(Qt.AlignCenter)  # set alignment to center
        font2 = QFont()
        font2.setPointSize(30)  # set font size to 30
        self.labelStart.setFont(font2)  # set font for the label
        layout6.addWidget(self.labelStart)

        self.container_list = QListWidget()
        #self.refreshContainerList()

        self.label1 = QLabel("Container - Status")

        layout6.addWidget(self.label1)
        layout6.addWidget(self.container_list)

        # initialize the list
        self.timer3 = QTimer()
        self.timer3.timeout.connect(lambda: self.containerStatus(self.container_list))
        self.timer3.timeout.connect(lambda: self.checkContainer())
        self.timer3.start(1000)

        self.setLayout(layout6)

    def containerStatus(self, containerList):
        containerStatusController.containerStatusC(self,containerList)

    def checkContainer(self):
        if containerE.containersPresent(self): 
            self.timer3.stop()
            current_widget_index = self.stackedWidget.currentIndex()
            current_widget = self.stackedWidget.widget(current_widget_index)
            self.stackedWidget.removeWidget(current_widget)
            #print("Container is present, going to page 1")
            self.stackedWidget.setCurrentIndex(1)
    def closeEvent(self):
        super().closeEvent()





class MainWindow(QMainWindow):
    def __init__(self):
        #Mainwindow
        super().__init__()
        self.setWindowTitle("Main Window")
        self.resize(800,600)

        # create stacked widget for the different pages
        self.stackedWidget = QStackedWidget()
        self.setCentralWidget(self.stackedWidget)

        font = QtGui.QFont()
        font.setPointSize(16)

        self.page0 = Page0(self.stackedWidget)
        self.page1 = Page1(self.stackedWidget)
        self.page2 = Page2(self.stackedWidget)
        self.page3 = Page3(self.stackedWidget)
        self.page4 = getData(self.stackedWidget)
        #self.page5 = loadingPage(self.stackedWidget)
        #self.page6 = FirstSetupPage(self.stackedWidget)
        #self.page6 = Page6(self.stackedWidget)
        #self.page7 = getData(self.stackedWidget)
        #self.stackedWidget.addWidget(self.page0)

        # add pages to the main stacked widget
        self.stackedWidget.addWidget(self.page0)
        self.stackedWidget.addWidget(self.page1)
        self.stackedWidget.addWidget(self.page2)
        self.stackedWidget.addWidget(self.page3)
        self.stackedWidget.addWidget(self.page4)
        #self.stackedWidget.addWidget(self.page5)
        #self.stackedWidget.addWidget(self.page6)
        #self.stackedWidget.addWidget(self.page7)
        #self.stackedWidget.setCurrentIndex(6)

    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Confirm Exit',
                                    'Are you sure you want to exit?',
                                    QMessageBox.Yes | QMessageBox.No,
                                    QMessageBox.No)

        if reply == QMessageBox.Yes:
            #confirm = QMessageBox.information(self, "Exiting...", "Application is closing...")
            process = QProcess(None)
            command = r'docker-compose -f .\docker-compose.yml stop'
            process.start(command)
            process.waitForFinished()

            output = process.readAllStandardOutput().data().decode()
            #print(output)

            event.accept()
        else:
            event.ignore()
    

    #prompts a confirm close before closing app, and docker-compose stop
if __name__ == '__main__':
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec_()


#remove stop
#solve issue of importing csv files
#Check the exit that all containers stop
#Clear comments


