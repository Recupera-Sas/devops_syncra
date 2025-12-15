import web.download_saem_reports
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QMessageBox, QFileDialog

class Process_RPA(QtWidgets.QMainWindow):

    def __init__(self, folder_path, process_data):
       
        super().__init__()
        
        self.file_path_RPA = None
        self.folder_path = folder_path
        self.process_data = process_data
        self.row_count_RPA = None
        
        self.exec_process()
    
    def exec_process(self):
        
        self.process_data.pushButton_Partitions_BD_8.clicked.connect(self.reports_saem_error)
        self.process_data.pushButton_Select_File_13.clicked.connect(self.select_file_RPA)
    
    def count_csv_rows(self, file_path):
        encodings = ['utf-8', 'latin-1']
        for encoding in encodings:
            try:
                with open(file_path, 'r', newline='', encoding=encoding) as csv_file:
                    row_count = sum(1 for _ in csv_file)
                return row_count
            except FileNotFoundError:
                return None
            except Exception as e:
                continue
        return None

    def reports_saem_error(self):

        if self.file_path_RPA != None:
            self.reports_saem()
                
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con los ID a descargar de Saem.")
            Mbox_File_Error.exec()
            
    def select_file_RPA(self):
        self.file_path_RPA = QFileDialog.getOpenFileName()
        self.file_path_RPA = str(self.file_path_RPA[0])
        if self.file_path_RPA:
            
            if not self.file_path_RPA.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_RPA = self.count_csv_rows(self.row_count_RPA)
                if self.row_count_RPA is not None:
                    self.row_count_RPA = "{:,}".format(self.row_count_RPA)
                    self.process_data.label_Total_Registers_7.setText(f"{self.row_count_RPA}")
                    
    def reports_saem(self):
        
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la descarga de ID de Saem.")
        Mbox_In_Process.exec()
        
        self.Base = web.download_saem_reports.read_csv_lists_saem(self.file_path_RPA)

        Mbox_In_Process = QMessageBox() 
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Descarga de reportes ejecutada exitosamente.")
        Mbox_In_Process.exec()