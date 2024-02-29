from flask import Flask, render_template, request, send_file, send_from_directory
from werkzeug.utils import secure_filename
import pandas as pd
import os
from io import BytesIO
import zipfile
import shutil

app = Flask(__name__)

# Configuração para o diretório de upload
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Configuração para o diretório temporário
TMP_FOLDER = 'tmp'
app.config['TMP_FOLDER'] = TMP_FOLDER

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Verifica se a pasta de upload existe, se não, cria
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])

        # Cria um DataFrame vazio para armazenar os dados
        all_files_data = pd.DataFrame()

        # Loop através de todos os arquivos enviados
        for uploaded_file in request.files.getlist('files'):
            if uploaded_file.filename != '':
                filename = secure_filename(uploaded_file.filename)
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                uploaded_file.save(file_path)

                # Lê o arquivo CSV e armazena os dados em um DataFrame
                df = pd.read_csv(file_path, delimiter=';')

                # Aplica as condições para DAT_CREDITO e DAT_CONFIRMACAO
                df['DAT_CREDITO'] = df['DATA_LANCAMENTO']
                df['DAT_CREDITO'] = df['DAT_CREDITO'].where(df['SITUACAO_PROPOSTA'] == 'INT', [None] * len(df))

                df['DAT_CONFIRMACAO'] = df['DATA_LANCAMENTO']
                df['DAT_CONFIRMACAO'] = df['DAT_CONFIRMACAO'].where(df['SITUACAO_PROPOSTA'] == 'INT', [None] * len(df))

                # Reordena as colunas conforme o layout especificado
                df = df.rename(columns={
                    'PROPOSTA': 'NUM_PROPOSTA',
                    'OPERACAO': 'NUM_CONTRATO',
                    'TIPO_OPERACAO': 'DSC_TIPO_PROPOSTA_EMPRESTIMO',
                    'COD_CONVENIO': 'COD_PRODUTO',
                    'NOME_CONVENIO': 'DSC_PRODUTO',
                    'DATA_CADASTRO': 'DAT_CTR_INCLUSAO',
                    'SITUACAO_PROPOSTA': 'DSC_SITUACAO_EMPRESTIMO',
                    'COD_USUARIO': 'NIC_CTR_USUARIO',
                    'CPF_CLIENTE': 'COD_CPF_CLIENTE',
                    'NOMECLI': 'NOM_CLIENTE',
                    'DATA_NASCIMENTO': 'DAT_NASCIMENTO',
                    'QTD_PARCELAS': 'QTD_PARCELA',
                    'VALOR_PARCELA': 'VAL_PRESTACAO',
                    'VALOR_FINANCIADO': 'VAL_BRUTO',
                    'VLR_LIB1': 'VAL_LIQUIDO',
                    'CODIGO_PROMOTORA': 'COD_LOJA_DIGITACAO',
                    'DAT_CREDITO': 'DAT_CREDITO',
                    'DAT_CONFIRMACAO': 'DAT_CONFIRMACAO'
                })

                df['DAT_EMPRESTIMO'] = df['DAT_CTR_INCLUSAO']

                # Concatena o DataFrame atual com o DataFrame principal
                all_files_data = pd.concat([all_files_data, df])

        # Cria um novo DataFrame com o layout especificado
        new_df = pd.DataFrame({
            'NUM_BANCO': ['623'] * len(all_files_data),
            'NOM_BANCO': ['BANCO PAN'] * len(all_files_data),
            'NUM_PROPOSTA': all_files_data['NUM_PROPOSTA'],
            'NUM_CONTRATO': all_files_data['NUM_CONTRATO'],
            'DSC_TIPO_PROPOSTA_EMPRESTIMO': all_files_data['DSC_TIPO_PROPOSTA_EMPRESTIMO'],
            'COD_PRODUTO': all_files_data['COD_PRODUTO'],
            'DSC_PRODUTO': all_files_data['DSC_PRODUTO'],
            'DAT_CTR_INCLUSAO': all_files_data['DAT_CTR_INCLUSAO'],
            'DSC_SITUACAO_EMPRESTIMO': all_files_data['DSC_SITUACAO_EMPRESTIMO'],
            'DAT_EMPRESTIMO': all_files_data['DAT_EMPRESTIMO'],
            'COD_EMPREGADOR': [None] * len(all_files_data),
            'DSC_CONVENIO': [None] * len(all_files_data),
            'COD_ORGAO': all_files_data['NOME_ORGAO'],
            'NOM_ORGAO': [None] * len(all_files_data),
            'COD_PRODUTOR_VENDA': [None] * len(all_files_data),
            'NOM_PRODUTOR_VENDA': [None] * len(all_files_data),
            'NIC_CTR_USUARIO': all_files_data['NIC_CTR_USUARIO'],
            'COD_CPF_CLIENTE': all_files_data['COD_CPF_CLIENTE'],
            'NOM_CLIENTE': all_files_data['NOM_CLIENTE'],
            'DAT_NASCIMENTO': all_files_data['DAT_NASCIMENTO'],
            'NUM_IDENTIDADE': [None] * len(all_files_data),
            'NOM_LOGRADOURO' : [None] * len(all_files_data),
            'NUM_PREDIO' : [None] * len(all_files_data),
            'DSC_CMPLMNT_ENDRC' : [None] * len(all_files_data),
            'NOM_BAIRRO' : [None] * len(all_files_data),
            'NOM_LOCALIDADE' : [None] * len(all_files_data),
            'SIG_UNIDADE_FEDERACAO' : [None] * len(all_files_data),
            'COD_ENDRCMNT_PSTL' : [None] * len(all_files_data),
            'NUM_TELEFONE' : [None] * len(all_files_data),
            'NUM_TELEFONE_CELULAR' : [None] * len(all_files_data),
            'NOM_MAE' : [None] * len(all_files_data),
            'NOM_PAI' : [None] * len(all_files_data),
            'NUM_BENEFICIO' : [None] * len(all_files_data),
            'QTD_PARCELA': all_files_data['QTD_PARCELA'],
            'VAL_PRESTACAO' : all_files_data['VAL_PRESTACAO'],
            'VAL_BRUTO': all_files_data['VAL_BRUTO'],
            'VAL_SALDO_RECOMPRA': [None] * len(all_files_data),
            'VAL_SALDO_REFINANCIAMENTO': [None] * len(all_files_data),
            'VAL_LIQUIDO' : all_files_data['VAL_LIQUIDO'],
            'PCR_PMT_PAGO_REF': [None] * len(all_files_data),
            'DAT_CREDITO' : all_files_data['DAT_CREDITO'],
            'DAT_CONFIRMACAO' : all_files_data['DAT_CONFIRMACAO'],
            'VAL_REPASSE': [None] * len(all_files_data),
            'PCL_COMISSAO': [None] * len(all_files_data),
            'VAL_COMISSAO': [None] * len(all_files_data),
            'COD_UNIDADE_EMPRESA' : [None] * len(all_files_data),
            'COD_SITUACAO_EMPRESTIMO' : [None] * len(all_files_data),
            'DAT_ESTORNO' : [None] * len(all_files_data),
            'DSC_OBSERVACAO' : [None] * len(all_files_data),
            'NUM_CPF_AGENTE' : [None] * len(all_files_data),
            'NUM_OBJETO_ECT' : [None] * len(all_files_data),
            'PCL_TAXA_EMPRESTIMO' : [None] * len(all_files_data),
            'DSC_TIPO_FORMULARIO_EMPRESTIMO' : 'DIGITAL',
            'DSC_TIPO_CREDITO_EMPRESTIMO' : [None] * len(all_files_data),
            'NOM_GRUPO_UNIDADE_EMPRESA' : [None] * len(all_files_data),
            'COD_PROPOSTA_EMPRESTIMO' : [None] * len(all_files_data),
            'COD_GRUPO_UNIDADE_EMPRESA' : [None] * len(all_files_data),
            'COD_TIPO_FUNCAO' : [None] * len(all_files_data),
            'COD_TIPO_PROPOSTA_EMPRESTIMO' : [None] * len(all_files_data),
            'COD_LOJA_DIGITACAO' : all_files_data['COD_LOJA_DIGITACAO'],
            'VAL_SEGURO' : [None] * len(all_files_data)
        })

        new_df['DAT_CTR_INCLUSAO'] = pd.to_datetime(all_files_data['DAT_CTR_INCLUSAO'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y')
        new_df['DAT_EMPRESTIMO'] = pd.to_datetime(all_files_data['DAT_EMPRESTIMO'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y')
        new_df['DAT_NASCIMENTO'] = pd.to_datetime(all_files_data['DAT_NASCIMENTO'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y')
        new_df['DAT_CREDITO'] = pd.to_datetime(all_files_data['DAT_CREDITO'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y')
        new_df['DAT_CONFIRMACAO'] = pd.to_datetime(all_files_data['DAT_CONFIRMACAO'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y')


        # Verifica se há mais de 20 mil linhas no DataFrame
        if len(new_df) > 20000:
            # Calcula o número de arquivos necessários
            num_files = -(-len(new_df) // 20000)  # Divisão arredondada para cima

            # Divide o DataFrame em partes menores
            dfs = [new_df[i*20000:(i+1)*20000] for i in range(num_files)]

            # Salva cada parte em um arquivo Excel separado
            output_files = []
            for i, df_part in enumerate(dfs):
                output_part = BytesIO()
                writer = pd.ExcelWriter(output_part, engine='openpyxl')
                df_part.to_excel(writer, index=False)
                writer.close()  # Fecha o escritor para salvar as alterações
                output_part.seek(0)

                # Define o nome do arquivo com o número da parte
                file_name = f'output_test_part_{i+1}.xlsx'
                temp_folder = 'tmp'  # Caminho relativo para a pasta tmp

                # Verifica se a pasta temporária existe, se não, cria
                if not os.path.exists(temp_folder):
                    os.makedirs(temp_folder)

                # Salva a parte em um arquivo temporário
                temp_file_path = f'tmp/{file_name}'
                with open(temp_file_path, 'wb') as temp_file:
                    temp_file.write(output_part.getvalue())
                                
                output_files.append(os.path.basename(temp_file_path))  # Adiciona apenas o nome do arquivo à lista

            return send_files(output_files)

        # Se o DataFrame tiver 20 mil linhas ou menos, envia um único arquivo
        else:
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='openpyxl')
            new_df.to_excel(writer, index=False)
            writer.close()  # Fecha o escritor para salvar as alterações
            output.seek(0)
            return send_file(
                output,
                as_attachment=True,
                download_name='output_test.xlsx',
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

    return render_template('upload.html')


def send_files(files):
    """Envia vários arquivos para download."""
    # Compacta todos os arquivos na pasta tmp em um arquivo zip
    zip_path = os.path.join(app.config['TMP_FOLDER'], 'Empilhamento_Pan.zip')
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in files:
            file_path = os.path.join(app.config['TMP_FOLDER'], file)
            zipf.write(file_path, os.path.basename(file_path))

    # Envia o arquivo zip para download
    return send_file(zip_path, as_attachment=True, download_name='Empilhamento_Pan.zip')

@app.route('/download_all')

def download_all():
    """Baixa todos os arquivos da pasta tmp em um arquivo zip e depois exclui os arquivos da pasta tmp."""
    files = os.listdir(app.config['TMP_FOLDER'])
    zip_path = os.path.join(app.config['TMP_FOLDER'], 'Empilhamento_Pan.zip')
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in files:
            file_path = os.path.join(app.config['TMP_FOLDER'], file)
            zipf.write(file_path, os.path.basename(file_path))

    # Envia o arquivo zip para download
    response = send_file(zip_path, as_attachment=True, download_name='Empilhamento_Pan.zip')

    # Remove cada arquivo temporário após o download do arquivo zip
    for file in files:
        file_path = os.path.join(app.config['TMP_FOLDER'], file)
        os.remove(file_path)

    # Remove a pasta temporária
    os.rmdir(app.config['TMP_FOLDER'])

    return response

@app.route('/delete_temp_files', methods=['GET'])
def delete_temp_files():
    """Exclui todos os arquivos da pasta tmp."""
    files = os.listdir(app.config['TMP_FOLDER'])
    for file in files:
        file_path = os.path.join(app.config['TMP_FOLDER'], file)
        os.unlink(file_path)  # Exclui o arquivo individualmente
    return 'Temporary files deleted successfully.'

if __name__ == '__main__':
    app.run(debug=True)