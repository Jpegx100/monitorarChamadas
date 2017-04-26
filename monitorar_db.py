import pymssql, psycopg2, time, threading
from datetime import datetime
from xml.dom.minidom import parse

def main():
	configs = load_config()
	last_date = load_last_date()
	if(configs):
		update_database(configs['server-origin'], configs['server-destiny'], configs['time-to-update'], 
			last_date, configs['default-pa'], configs['default-user'])
        
def update_database(server_origin, server_destiny, time_to_update, last_date, default_pa, default_user):
	data = get_data_from_view(last_date, server_origin)
	ids = [r['cdChamado'] for r in data]
	print(str(len(ids))+" Chamadados encontrados: "+str(ids))
	
	mapped_data = map_data(data, server_destiny)
	ids = [r['cdChamado'] for r in mapped_data]
	datas = [int(r['dtOver'].strftime("%s"))*1000 for r in mapped_data]
	print(str(len(ids))+" Chamadados validos: "+str(ids))
	print("Datas: "+str(datas))
	
	insert_data_in_db(mapped_data, server_destiny, default_pa, default_user)
	new_last_date = max_date(mapped_data, 'dtOver')
	save_last_date(new_last_date)
	print("Maior data: "+str(new_last_date))
	print("CONCLUIDO CICLO")
	new_last_date = datetime.fromtimestamp(int(new_last_date) / 1000.0)

	print(get_last_id_from_view(server_origin))
	threading.Timer(time_to_update, update_database, [server_origin, server_destiny, time_to_update, new_last_date, default_pa, default_user]).start()

def max_date(rows, key):
	bigger = 0
	for r in rows:
		if key in r.keys() and r[key]!=None:
			dt = int(r[key].strftime("%s"))*1000
			if dt > bigger:
				bigger = dt
	return bigger

def load_config():
	try:
		configs = {}
		configs['server-origin'] = {}
		configs['server-destiny'] = {}
		dom = parse('config.xml')

		# Load server-origin values
		server_origin = dom.getElementsByTagName('server-origin')[0]
		configs['server-origin']['ip'] = server_origin.getElementsByTagName('ip')[0].firstChild.data
		configs['server-origin']['user'] = server_origin.getElementsByTagName('user')[0].firstChild.data
		configs['server-origin']['password'] = server_origin.getElementsByTagName('password')[0].firstChild.data
		configs['server-origin']['database'] = server_origin.getElementsByTagName('database')[0].firstChild.data

		# Load server-destiny values
		server_destiny = dom.getElementsByTagName('server-destiny')[0]
		configs['server-destiny']['ip'] = server_destiny.getElementsByTagName('ip')[0].firstChild.data
		configs['server-destiny']['user'] = server_destiny.getElementsByTagName('user')[0].firstChild.data
		configs['server-destiny']['password'] = server_destiny.getElementsByTagName('password')[0].firstChild.data
		configs['server-destiny']['database'] = server_destiny.getElementsByTagName('database')[0].firstChild.data

		# Load time to update
		configs['time-to-update'] = int(dom.getElementsByTagName('time-to-update')[0].firstChild.data)

		# Load default values
		configs['default-pa'] = int(dom.getElementsByTagName('default-pa')[0].firstChild.data)
		configs['default-user'] = int(dom.getElementsByTagName('default-user')[0].firstChild.data)

		return configs
	
	except Exception as e:
		print(e)
		return None

def save_last_date(last_date):
	f = open('info.xml', 'w')
	f.write("<last-call-date>"+str(last_date)+"</last-call-date>")
	f.close()

def load_last_date():
	try:
		dom = parse('info.xml')
		last_date = int(dom.getElementsByTagName('last-call-date')[0].firstChild.data)
		return datetime.fromtimestamp(int(last_date) / 1000.0)
	except Exception as e:
		save_last_date(str(datetime.fromtimestamp(0)))
		print(e)
		return datetime.fromtimestamp(0)

def get_data_from_view(last_date, server):
	'''Connect to VIEW'''
	date_str = str(last_date)
	fields = "nrTelefone, dsNomeSolicitante, dsLogradouroOrigem, dsReferenciaOrigem, "+\
			"dsComplementoOrigem, dsBairroOrigem, nrLatOrigem, nrLngOrigem, cdChamado, "+\
			"dtChamado, dtCancelamento, dtCadastro, dsStatus, dsPlaca, dtFinal"
	select_query = "SELECT "+fields+" FROM [taxidigital_oaz].[dbo].[300_VwLstChamado] WHERE "
	queryCance = select_query+"dsStatus='Cancelado' AND dtCancelamento IS NOT NULL AND CAST(dtCancelamento AS DATETIME) > '"+date_str+"'"
	queryFinal = select_query+"dsStatus='Final' AND dtFinal IS NOT NULL AND CAST(dtFinal AS DATETIME) > '"+date_str+"'"
	
	conn = pymssql.connect(
	    host=server['ip'],
	    user=server['user'],
	    password=server['password'],
	    database=server['database']
	)
	cursor = conn.cursor(as_dict=True)
	cursor.execute(queryCance+" UNION "+queryFinal)
	rows = [r for r in cursor]
	conn.close()

	return rows

def get_connection(server):
	conn_string = "host="+server['ip']+" dbname="+server['database']+" user="+server['user']+" password="+server['password']
	conn = psycopg2.connect(conn_string)
	return conn

def get_next_chamada_id(server):
	try:
		conn = get_connection(server)
		cursor = conn.cursor()
		cursor.execute("SELECT nextval('chamada_id_seq');")
		row = [r for r in cursor]
		if len(row)>0:
			return row[0][0]
	except Exception as e:
		print(e)
	return None

def insert_data_in_db(rows, server, ponto_apoio_default, usuario_atend_default):
	'''Connect to POSTGRES'''
	conn = get_connection(server)
	cursor = conn.cursor()
	fields = 'id, fone, nome, logradouro, referencia, "dataChamada", "horaChamada", "dataSolicitacao",'+\
			'"horaSolicitacao", complemento, bairro, situacao, latitude, longitude, "id_chamado_digital", "idUnidade", ' +\
			' "idUsuarioAtend", solicitada, passagem, status, "idPontoApoio"'
	for row in rows:
		if(not id_chamada_duplicada(server, str(row["cdChamado"]))):
			try:
				chamada_id = str(get_next_chamada_id(server))
				if row["dataCanc"]!=None:                                                
					cursor.execute("INSERT INTO chamadas("+fields+', "dataCanc", "horaCanc"' +") VALUES ("+chamada_id+","+
						row["fone"]+",'"+row["nome"]+"','"+row["logradouro"]+"','"+row["referencia"]+"',"+
						row["dataChamada"].strftime("'%Y-%m-%d'")+","+row["horaChamada"]+","+
                                                row["dataSolicitacao"].strftime("'%Y-%m-%d'")+","+row["horaSolicitacao"]+",'"+
						row["complemento"]+"','"+row["bairro"]+"','"+row["situacao"]+"',"+str(row["latitude"])+","+
						str(row["longitude"])+", "+str(row["cdChamado"])+", "+str(row["idUnidade"])+","+
						str(usuario_atend_default)+", 'N', 'N', 'F', "+str(ponto_apoio_default)+",'"+str(row["dataCanc"])+"',"+
						row["horaCanc"]+");")
				else:                                            
					cursor.execute("INSERT INTO chamadas("+fields+") VALUES ("+chamada_id+","+
						row["fone"]+",'"+row["nome"]+"','"+row["logradouro"]+"','"+row["referencia"]+"',"+
						row["dataChamada"].strftime("'%Y-%m-%d'")+","+row["horaChamada"]+","+
                                                row["dataSolicitacao"].strftime("'%Y-%m-%d'")+","+row["horaSolicitacao"]+",'"+
						row["complemento"]+"','"+row["bairro"]+"','"+row["situacao"]+"',"+str(row["latitude"])+","+
						str(row["longitude"])+", "+str(row["cdChamado"])+", "+str(row["idUnidade"])+","+
						str(usuario_atend_default)+", 'N', 'N', 'F', "+str(ponto_apoio_default)+");")
			except Exception as e:
				print(e)
		else:
			print("Chamado "+str(row['cdChamado'])+" duplicado não foi inserido.")

	conn.commit()
	conn.close()
	max_data = max_date(rows, "dtOver")
	print(max_data)
	save_last_date(max_data)

def get_hour_from_date(date):
	return str(date.hour)+str(date.minute)+str(date.second)

def get_id_unidade(placa, server):
	try:
		if not placa: return None
		conn = get_connection(server)
		cursor = conn.cursor()
		cursor.execute("SELECT id FROM unidades where placa='"+placa.upper()+"'")
		rows = [r for r in cursor]
		conn.close()
		if(len(rows)>0):
			return rows[0][0]
	except Exception as e:
		print(str(e))

	return None

def parse_Situation(situation):
	if situation.lower() == 'final': return 'P'
	if situation.lower() == 'cancelado': return 'C'

def map_data(rows, server):
	new_rows = []
	for r in rows:
		try:
			new_row = {}
			num_unidade = get_id_unidade(r['dsPlaca'], server)
			if(num_unidade!=None and r['dtChamado'] and r['dtCadastro']):
				new_row['fone'] = r['nrTelefone'] if len(r['nrTelefone'])<=11 else r['nrTelefone'][:11]
				new_row['nome'] = r['dsNomeSolicitante']
				new_row['logradouro'] = r['dsLogradouroOrigem'] if len(r['dsLogradouroOrigem'])<=70 else r['dsLogradouroOrigem'][:70]
				new_row['referencia'] = r['dsReferenciaOrigem'] if len(r['dsReferenciaOrigem'])<=60 else r['dsReferenciaOrigem'][:60]
				new_row['dataChamada'] = r['dtChamado'].date()
				new_row['horaChamada'] = get_hour_from_date(r['dtChamado']) if r['dtChamado'] else None
				new_row['dataSolicitacao'] = r['dtCadastro'].date()
				new_row['horaSolicitacao'] = get_hour_from_date(r['dtCadastro']) if r['dtCadastro'] else None
				new_row['complemento'] = r['dsComplementoOrigem']
				new_row['bairro'] = r['dsBairroOrigem']
				new_row['situacao'] = parse_Situation(r['dsStatus'])
				new_row['latitude'] = r['nrLatOrigem']
				new_row['longitude'] = r['nrLngOrigem']
				new_row['idUnidade'] = num_unidade
				new_row['cdChamado'] = r['cdChamado']

				if('dtCancelamento' in r.keys() and r['dtCancelamento']):
					new_row['dataCanc'] = r['dtCancelamento'].date()
					new_row['horaCanc'] = get_hour_from_date(r['dtCancelamento']) if r['dtCancelamento'] else None
					new_row['dtOver'] = r['dtCancelamento']
					new_rows.append(new_row)
					continue

				if('dtFinal' in r.keys() and r['dtFinal']):
					new_row['dtFinal'] = r['dtFinal'].date()
					new_row['dtOver'] = r['dtFinal']

					#Insert dataCanc
					new_row['dataCanc'] = None
					new_row['horaCanc'] = None
					
					new_rows.append(new_row)
					continue

		except Exception as e:
			print(str(e))
	return new_rows

def id_chamada_duplicada(server, taxi_digital_id):
	try:
		conn = get_connection(server)
		cursor = conn.cursor()
		cursor.execute("SELECT id_chamado_digital from chamadas where id_chamado_digital="+taxi_digital_id+";")
		row = [r for r in cursor]
		if len(row)>0:
			conn.close()
			return True
	except Exception as e:
		print(e)
	return False

def get_last_id_from_view(server):
	conn = pymssql.connect(
	    host=server['ip'],
	    user=server['user'],
	    password=server['password'],
	    database=server['database']
	)
	cursor = conn.cursor()
	cursor.execute("SELECT MAX(cdChamado) FROM [taxidigital_oaz].[dbo].[300_VwLstChamado]")
	return [r for r in cursor]

def show(data):
	for x in data:
		print(x['dsStatus'])
if __name__ == "__main__":
 	main()
