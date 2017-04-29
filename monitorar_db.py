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
	try:
		data = get_data_from_view(last_date, server_origin)
		try:
			print(str(len(data) - 1)+" Chamadados encontrados")
		except Exception as e:
			print("0 Chamadas encontrados")
		mapped_data = map_data(data, server_destiny)
		insert_data_in_db(mapped_data, server_destiny, default_pa, default_user)		
		new_last_date = max_date(mapped_data, 'dtOver')
		save_last_date(new_last_date)
		new_last_date = datetime.fromtimestamp(int(new_last_date) / 1000.0)
		threading.Timer(time_to_update, update_database, [server_origin, server_destiny, time_to_update, new_last_date, default_pa, default_user]).start()

	except Exception as e:
		print(e)
		print("Reiniciando servico")
		threading.Timer(time_to_update, update_database, [server_origin, server_destiny, time_to_update, last_date, default_pa, default_user]).start()

def get_data_from_view(last_date, server):
	'''Connect to VIEW'''
	try:
		date_str = str(last_date)
		fields = "nrTelefone, dsNomeSolicitante, dsLogradouroOrigem, dsReferenciaOrigem, "+\
			"dsComplementoOrigem, dsBairroOrigem, nrLatOrigem, nrLngOrigem, cdChamado, "+\
			"dtChamado, dtCancelamento, dtCadastro, dsStatus, dsPlaca, dtFinal, nrChamado, dsSiglaMoto"
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
	except Exception as e:
		print(e)
		print("\n"+queryFinal+"\n")

def map_data(rows, server):
	new_rows = []
	j = 0
	i = 0
	for r in rows:
		i = i+1
		print("Mapped "+str(i)+" of "+str(len(rows)))
		try:
			new_row = {}
			if(r['dtChamado'] and r['dtCadastro'] and r['dsSiglaMoto']):
				hora_ch = get_hour_from_date(r['dtChamado']) if r['dtChamado'] else None
				new_row['fone'] = r['nrTelefone'] if len(r['nrTelefone'])<=11 else r['nrTelefone'][:11]
				new_row['nome'] = r['dsNomeSolicitante'] if len(r['dsNomeSolicitante'])<=50 else r['dsNomeSolicitante'][:50]
				new_row['logradouro'] = r['dsLogradouroOrigem'] if len(r['dsLogradouroOrigem'])<=70 else r['dsLogradouroOrigem'][:70]
				new_row['referencia'] = r['dsReferenciaOrigem'] if len(r['dsReferenciaOrigem'])<=60 else r['dsReferenciaOrigem'][:60]
				new_row['dataChamada'] = r['dtChamado'].date()
				new_row['horaChamada'] = hora_ch
				new_row['dataSolicitacao'] = r['dtCadastro'].date()
				new_row['horaSolicitacao'] = get_hour_from_date(r['dtCadastro']) if r['dtCadastro'] else None
				new_row['complemento'] = r['dsComplementoOrigem'] if len(r['dsComplementoOrigem'])<=80 else r['dsComplementoOrigem'][:80]
				new_row['bairro'] = r['dsBairroOrigem'] if len(r['dsBairroOrigem'])<=50 else r['dsBairroOrigem'][:50]
				new_row['situacao'] = parse_Situation(r['dsStatus'])
				new_row['latitude'] = r['nrLatOrigem']
				new_row['longitude'] = r['nrLngOrigem']
				new_row['idUnidade'] = r['dsSiglaMoto']
				new_row['cdChamado'] = r['nrChamado']

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
			else:
				if r['dtChamado']==None:
					print("Data do chamado não encontrada ")
					j = j+1
					continue
				if r['dtCadastro']==None:
					print("Data do cadsatro não encontrada ")
					j = j+1
					continue
				if r['dsSiglaMoto']==None:
					print("dsSiglaMoto não existe")
					j = j+1
					continue

		except Exception as e:
			print(str(e))
	print("Chamadas não incluidas: "+str(j))
	return new_rows

def insert_data_in_db(rows, server, ponto_apoio_default, usuario_atend_default):
	'''Connect to POSTGRES'''
	conn = get_connection(server)
	cursor = conn.cursor()
	fields = 'id, fone, nome, logradouro, referencia, "dataChamada", "horaChamada", "dataSolicitacao",'+\
			'"horaSolicitacao", complemento, bairro, situacao, latitude, longitude, "id_chamado_digital", "idUnidade", ' +\
			' "idUsuarioAtend", solicitada, passagem, status, "idPontoApoio", bandeira'
	i = 0
	for row in rows:
		i = i+1
		id_chamada = str(get_next_chamada_id(server))
		if(row["idUnidade"] and not id_chamada_duplicada(server, str(row["idUnidade"]))):
			if(exist_unidade(server, str(row['idUnidade']))):
				try:
					bandeira = get_bandeira(server, row["dataChamada"], row["horaSolicitacao"][:4])
					# In case of call been canceled
					if row["dataCanc"]!=None:                                                
						cursor.execute("INSERT INTO chamadas("+fields+', "dataCanc", "horaCanc"' +") VALUES ("+id_chamada+","+
							row["fone"]+",'"+row["nome"]+"','"+row["logradouro"]+"','"+row["referencia"]+"',"+
							row["dataChamada"].strftime("'%Y-%m-%d'")+",'"+row["horaChamada"]+"',"+
	                                                row["dataSolicitacao"].strftime("'%Y-%m-%d'")+",'"+row["horaSolicitacao"]+"','"+
							row["complemento"]+"','"+row["bairro"]+"','"+row["situacao"]+"',"+str(row["latitude"])+","+
							str(row["longitude"])+", "+str(row["cdChamado"])+", "+str(row["idUnidade"])+","+
							str(usuario_atend_default)+", 'N', 'N', 'F', "+str(ponto_apoio_default)+",'"+bandeira+"','"+str(row["dataCanc"])+"','"+
							row["horaCanc"]+"');")
					# In case of call been finalized
					else:                                            
						cursor.execute("INSERT INTO chamadas("+fields+") VALUES ("+id_chamada+","+
							row["fone"]+",'"+row["nome"]+"','"+row["logradouro"]+"','"+row["referencia"]+"',"+
							row["dataChamada"].strftime("'%Y-%m-%d'")+",'"+row["horaChamada"]+"',"+
	                                                row["dataSolicitacao"].strftime("'%Y-%m-%d'")+",'"+row["horaSolicitacao"]+"','"+
							row["complemento"]+"','"+row["bairro"]+"','"+row["situacao"]+"',"+str(row["latitude"])+","+
							str(row["longitude"])+", "+str(row["cdChamado"])+", "+str(row["idUnidade"])+","+
							str(usuario_atend_default)+", 'N', 'N', 'F', "+str(ponto_apoio_default)+",'"+bandeira+"');")
				except Exception as e:
					print(e)
			else:
				print("Unidade "+str(row['idUnidade'])+" não cadastrada no banco local")
		else:
			if row["idUnidade"]==None:
				print("Chamada não incluida por não possuir idUnidade")
			chamada_duplicada = get_chamada(server, str(row["cdChamado"]))
			chamada_id = get_next_chamada_id(server)
			# In case of call was Canceled and will be Finalized
			if (chamada_duplicada[0][9]=='C') and (row["situacao"]=='P'):
				cursor.execute("UPDATE chamadas SET situacao='"+row['situacao']+"' WHERE 'idUnidade'="+str(chamada_id)+");")
			#else:
			#	print("Chamado "+str(row['cdChamado'])+" duplicado não foi inserido.")
		print(str(i)+" inserted of "+str(len(rows)))
	conn.commit()
	conn.close()
	# max_data = max_date(rows, "dtOver")
	# save_last_date(max_data)

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
		conn.close()
	except Exception as e:
		print(e)
	return None

def get_hour_from_date(date):
	hour = str(date.hour) if len(str(date.hour))>1 else "0"+str(date.hour)
	minute = str(date.minute) if len(str(date.minute))>1 else "0"+str(date.minute)
	second = str(date.second) if len(str(date.second))>1 else "0"+str(date.second)
	hms = hour+minute+second
	return hms

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

def exist_unidade(server, id_unidade):
	try:
		conn = get_connection(server)
		cursor = conn.cursor()
		cursor.execute("SELECT id from unidades where id="+id_unidade+";")
		row = [r for r in cursor]
		conn.close()
		if len(row)>0:
			return True
	except Exception as e:
		print(e)
	return False

def id_chamada_duplicada(server, taxi_digital_id):
	try:
		conn = get_connection(server)
		cursor = conn.cursor()
		cursor.execute("SELECT id_chamado_digital from chamadas where id_chamado_digital="+taxi_digital_id+";")
		row = [r for r in cursor]
		conn.close()
		if len(row)>0:
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
	row = cursor.fetchone()
	conn.close()
	return 

def show(data):
	for x in data:
		print(x['dsStatus'])

def get_chamada(server, taxi_digital_id):
	conn = get_connection(server)
	cursor = conn.cursor()
	cursor.execute("SELECT * from chamadas where id_chamado_digital="+taxi_digital_id+";")
	row = [r for r in cursor]
	conn.close()
	return row

def get_bandeira(destiny, date, horamin):
        month = str(date.month) if len(str(date.month))>1 else "0"+str(date.month)
        day = str(date.day) if len(str(date.day))>1 else "0"+str(date.day)

        intervals = get_bandeira2_intervalos(destiny)
        # Hollydays or sundays
        if is_hollyday(day+month, destiny) or date.weekday()==7:
                if intervals["domfer"][:4] < horamin and horamin < intervals["domfer"][4:]: return "2"
                else: return "1"
        else:
                # In case of be a day of week
                if date.weekday()<5:
                        if intervals["segsex"]["intervalo1"][:4] < horamin and horamin < intervals["segsex"]["intervalo1"][4:]: return "2"
                        if intervals["segsex"]["intervalo2"][:4] < horamin and horamin < intervals["segsex"]["intervalo2"][4:]: return "2"
                        return "1"
                # In case of be a weekend day
                else:
                        if intervals["sabado"]["intervalo1"][:4] < horamin and horamin < intervals["sabado"]["intervalo1"][4:]: return "2"
                        if intervals["sabado"]["intervalo2"][:4] < horamin and horamin < intervals["sabado"]["intervalo2"][4:]: return "2"
                        return "1"

def get_bandeira2_intervalos(server):
        conn = get_connection(server)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bandeiras")
        rows = [r for r in cursor]
        conn.close()
        return {"segsex": {"intervalo1": rows[0][0][:8], "intervalo2": rows[0][0][8:]},
                "sabado": {"intervalo1": rows[0][1][:8], "intervalo2": rows[0][1][8:]},
                "domfer": rows[0][2]}

def is_hollyday(date, server):
        conn = get_connection(server)
        cursor = conn.cursor()
        cursor.execute("SELECT data FROM feriados WHERE data='"+date+"'")
        rows = [r for r in cursor]
        conn.close()
        if len(rows)>0: 
        	return True
        else: return False


if __name__ == "__main__":
 	main()
#configs = load_config()
#last_date = load_last_date()
#origin = configs["server-origin"]
#destiny = configs["server-destiny"]
#data = get_data_from_view(last_date, origin)
#mapped_data = map_data(data, destiny)
