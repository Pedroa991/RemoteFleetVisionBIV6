"""GUI para do APP PowerProfile"""

import webbrowser
from tkinter import filedialog
from tkinter.messagebox import showerror, showinfo, showwarning
import customtkinter as ctk
import rfvbi


MAIN_WINDOW_TITLE = f"RFV TO BI {rfvbi.SCRIPT_VERSION}"
MIN_SIZE_WINDOW_WIDTH = 370
MIN_SIZE_WINDOW_HEIGHT = 200


class GadgetsFuntions:
    """Classe para funções dos botões"""

    def __init__(self):
        self.path_db = ""
        self.path_englog = ""
        self.path_eventslog = ""
        self.concat = ctk.IntVar()

    def getbd(self):
        """Pega o caminho do banco de dados"""
        dirname = filedialog.askdirectory(title="Selecione a pasta do BD do Cliente")
        if dirname:
            print("Pasta de Destino: " + str(dirname) + "\n")
            self.path_db = dirname

    def getenglog(self):
        """Pega caminho do arquivo de referências de performance dos motores"""
        pathfile = filedialog.askopenfilename(
            title="Selecione o arquivo de log de motores",
            filetypes=[("Zip file", "*.zip")],
        )
        if pathfile:
            print("Arquivo de motores:" + str(pathfile) + "\n")
            self.path_englog = pathfile

    def geteventslog(self):
        """Pega caminho do arquivo de eventos dos motores"""
        pathfile = filedialog.askopenfilename(
            title="Selecione o arquivo de log de eventos",
            filetypes=[("Excel file", "*.xls*")],
        )
        if pathfile:
            print("Arquivo de eventos:" + str(pathfile) + "\n")
            self.path_eventslog = pathfile

    def on_checkbutton_toggle(self):
        """Emite um aviso caso a opção concatenar seja desabilitada"""
        isconcat = self.concat.get()
        if not isconcat:
            showwarning(
                "Aviso",
                "Ao desmarcar a opção concatenar, o script irá excluir "
                + "todos os dados do banco de dados! "
                + "Mantendo somente os dados do último período selecionado!",
            )

    def run_rfvtobi(self) -> None:
        """Executa a rotina do RFV TO BI"""
        if not self.path_db:
            tx_error = "Erro: Caminho do BD Cliente inválido!"
            print(tx_error)
            showerror("Erro", tx_error)
            return

        if not self.path_englog:
            tx_error = "Erro: Caminho do arquivo de referência de motor inválido!"
            print(tx_error)
            showerror("Erro", tx_error)
            return

        if not self.path_eventslog:
            tx_error = "Erro: Caminho do arquivo de referência de motor inválido!"
            print(tx_error)
            showerror("Erro", tx_error)
            return

        rfvbi.main(
            self.path_db, self.path_englog, self.path_eventslog, self.concat.get()
        )
        showinfo("Sucesso!", "Resultados obitidos com sucesso!")


def put_gadgets_main(app: ctk.CTk) -> None:
    """Coloca gadgets da janela principal"""

    runbt = GadgetsFuntions()

    tx_title = "RFV TO BI"
    lb_title = ctk.CTkLabel(app, text=tx_title)
    lb_title.pack(side="top")

    bt_englog = ctk.CTkButton(
        master=app, text="Log de Motores", command=runbt.getenglog
    )
    bt_eventslog = ctk.CTkButton(
        master=app, text="Log de Eventos", command=runbt.geteventslog
    )
    bt_db = ctk.CTkButton(master=app, text="BD Cliente", command=runbt.getbd)
    bt_run = ctk.CTkButton(
        master=app, text="Executar", fg_color="Red", command=runbt.run_rfvtobi
    )

    cb_concat = ctk.CTkCheckBox(
        master=app,
        text="Concatenar",
        command=runbt.on_checkbutton_toggle,
        variable=runbt.concat,
    )

    bt_englog.place(relx=0.30, rely=0.30, anchor=ctk.CENTER)
    bt_eventslog.place(relx=0.70, rely=0.30, anchor=ctk.CENTER)
    bt_db.place(relx=0.30, rely=0.50, anchor=ctk.CENTER)
    bt_run.place(relx=0.50, rely=0.72, anchor=ctk.CENTER)

    cb_concat.select()
    cb_concat.place(relx=0.70, rely=0.50, anchor=ctk.CENTER)

    text_about = rfvbi.SCRIPT_VERSION + " - By Pedro Venancio - Sobre / Ajuda"
    lb_about = ctk.CTkLabel(app, text=text_about, text_color="blue")
    lb_about.pack(side="bottom")
    lb_about.bind(
        "<Button-1>",
        lambda x: webbrowser.open_new("https://www.linkedin.com/in/pedrobvenancio/"),
    )


def main() -> None:
    """Cria a janela principal"""

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    app = ctk.CTk()
    app.minsize(width=MIN_SIZE_WINDOW_WIDTH, height=MIN_SIZE_WINDOW_HEIGHT)
    app.title(MAIN_WINDOW_TITLE)
    app.geometry(f"{MIN_SIZE_WINDOW_WIDTH} X {MIN_SIZE_WINDOW_HEIGHT}")
    app.resizable(False, False)

    put_gadgets_main(app)

    app.mainloop()


if __name__ == "__main__":

    print(
        "Bem-vindo ao PowerProfile! \n",
        "Versão: ",
        rfvbi.SCRIPT_VERSION,
        "\n",
    )

    main()
