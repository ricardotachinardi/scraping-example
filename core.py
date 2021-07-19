import process_data
import email_module
import os
import time

def main():
    filename = "tabela_do_case.xlsx"
    
    print("Iniciando a execução")
    
    df = process_data.get_formatted_dataframe()
    print(df)
    df.to_excel(filename)
    
    print("Tabela obtida, enviando email")
    
    email_module.send_email(filename)

    os.remove(filename)
    
if __name__ == '__main__':
    main()