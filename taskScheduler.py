from serverConnections import database
import subprocess

# Consulta dos dados da tabela
with database('DW-REDSHIFT') as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT caminho_script, cron FROM caronte.agenda_pipelines where ativo = True")
        scripts = cur.fetchall()

# Criar as entradas cron
cron_entries = []
for script in scripts:
    caminho_script = script[0]
    cron_expression = script[1]
    cron_entry = f"{cron_expression} python {caminho_script}"
    cron_entries.append(cron_entry)

# Criar um único comando echo com todas as entradas cron
cron_content = '\n'.join(cron_entries)
echo_command = f"echo '{cron_content}' | crontab -u "

# Executar o comando echo para adicionar as entradas cron
subprocess.run(echo_command, shell=True)