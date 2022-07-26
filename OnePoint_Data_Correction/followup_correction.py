from OnePoint_Data_Correction.models import Follow_Up, session


query = session.query(Follow_Up).filter_by(key=81420).first()

print(query.txtDetails)

# query.txtDetails = ''
#
# session.commit()
# session.close()