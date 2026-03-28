from flask import Flask, render_template, request, jsonify, make_response
import sqlite3
import os
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

app = Flask(__name__)
app.config['SECRET_KEY'] = 'ahpc-indice-crimen-2026'
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ahpc_crimen.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ejecutar_consulta(query, params=()):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    resultados = cursor.fetchall()
    conn.close()
    return resultados

@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total FROM indice_crimen")
    total_registros = cursor.fetchone()['total']
    cursor.execute("SELECT MIN(año) as min, MAX(año) as max FROM indice_crimen WHERE año IS NOT NULL")
    rango_años = cursor.fetchone()
    cursor.execute("SELECT COUNT(DISTINCT causa) as total FROM indice_crimen WHERE causa IS NOT NULL")
    total_tipos = cursor.fetchone()['total']
    conn.close()
    
    año_inicio = rango_años['min'] if rango_años['min'] else '-'
    año_fin = rango_años['max'] if rango_años['max'] else '-'
    
    stats = {
        'total_registros': total_registros,
        'año_inicio': año_inicio,
        'año_fin': año_fin,
        'total_tipos': total_tipos
    }
    return render_template('index.html', stats=stats)

@app.route('/buscar')
def buscar():
    return render_template('buscar.html')

@app.route('/api/buscar', methods=['GET'])
def api_buscar():
    apellido = request.args.get('apellido', '').strip()
    nombre = request.args.get('nombre', '').strip()
    año_desde = request.args.get('año_desde', '').strip()
    año_hasta = request.args.get('año_hasta', '').strip()
    texto_libre = request.args.get('texto_libre', '').strip()
    
    query = "SELECT id, año, legajo, expediente, partes, causa FROM indice_crimen WHERE 1=1"
    params = []
    
    if apellido:
        query += " AND partes LIKE ?"
        params.append(f"%{apellido}%")
    if nombre:
        query += " AND partes LIKE ?"
        params.append(f"%{nombre}%")
    if año_desde:
        query += " AND año >= ?"
        params.append(str(año_desde))
    if año_hasta:
        query += " AND año <= ?"
        params.append(str(año_hasta))
    if texto_libre:
        query = """
            SELECT id, año, legajo, expediente, partes, causa 
            FROM indice_crimen 
            WHERE id IN (
                SELECT rowid FROM indice_crimen_fts WHERE indice_crimen_fts MATCH ?
            )
        """
        params = [texto_libre]
    
    query += " ORDER BY año DESC LIMIT 100"
    resultados = ejecutar_consulta(query, params)
    
    registros = []
    for row in resultados:
        registros.append({
            'id': row['id'],
            'año': row['año'],
            'apellido': row['partes'], 
            'nombre': '', 
            'tipo_acto': '',
            'caratula': row['causa'] if row['causa'] else '',
            'fojas': row['legajo'],
            'signatura': row['expediente']
        })
    
    return jsonify({
        'total': len(registros),
        'registros': registros
    })

@app.route('/detalle/<int:registro_id>')
def detalle(registro_id):
    query = "SELECT * FROM indice_crimen WHERE id = ?"
    resultados = ejecutar_consulta(query, (registro_id,))
    if not resultados:
        return "Registro no encontrado", 404
    registro = dict(resultados[0])
    return render_template('detalle.html', registro=registro)

@app.route('/api/exportar-pdf', methods=['GET'])
def exportar_pdf():
    apellido = request.args.get('apellido', '').strip()
    nombre = request.args.get('nombre', '').strip()
    año_desde = request.args.get('año_desde', '').strip()
    año_hasta = request.args.get('año_hasta', '').strip()
    texto_libre = request.args.get('texto_libre', '').strip()
    
    query = "SELECT id, año, legajo, expediente, partes, causa FROM indice_crimen WHERE 1=1"
    params = []
    
    if apellido:
        query += " AND partes LIKE ?"
        params.append(f"%{apellido}%")
    if nombre:
        query += " AND partes LIKE ?"
        params.append(f"%{nombre}%")
    if año_desde:
        query += " AND año >= ?"
        params.append(str(año_desde))
    if año_hasta:
        query += " AND año <= ?"
        params.append(str(año_hasta))
    if texto_libre:
        query = """
            SELECT id, año, legajo, expediente, partes, causa 
            FROM indice_crimen 
            WHERE id IN (
                SELECT rowid FROM indice_crimen_fts WHERE indice_crimen_fts MATCH ?
            )
        """
        params = [texto_libre]
    
    query += " ORDER BY año DESC LIMIT 1000"
    resultados = ejecutar_consulta(query, params)
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=18)
    elements = []
    
    styles = getSampleStyleSheet()
    title_style = styles['Heading1']
    title_style.alignment = 1 # Center
    
    elements.append(Paragraph("Resultados de Búsqueda - SERIE CRIMEN CAPITAL", title_style))
    elements.append(Spacer(1, 0.25 * inch))
    
    data = [['Año', 'Partes', 'Causa', 'Legajo', 'Expediente']]
    
    # Process paragraph wrappers for long text in cells
    normal_style = styles['Normal']
    normal_style.fontSize = 8
    
    for r in resultados:
        # reportlab Paragraph requires string coercion and newline replacement
        partes_text = str(r['partes']) if r['partes'] else ''
        causa_text = str(r['causa']) if r['causa'] else ''
        
        data.append([
            str(r['año']), 
            Paragraph(partes_text, normal_style), 
            Paragraph(causa_text, normal_style), 
            str(r['legajo']), 
            str(r['expediente'])
        ])
    
    t = Table(data, colWidths=[40, 160, 200, 60, 60])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0,1), (-1,-1), 8),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE')
    ]))
    
    elements.append(t)
    doc.build(elements)
    
    pdf = buffer.getvalue()
    buffer.close()
    
    response = make_response(pdf)
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = 'inline; filename=resultados_crimen.pdf'
    return response

@app.route('/api/apellidos', methods=['GET'])
def api_apellidos():
    # disabled or change logic to just return matches from partes
    return jsonify([])

@app.route('/estadisticas')
def estadisticas():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Top Causas (simulate "Tipos de Acto")
    cursor.execute("""
        SELECT causa as tipo_acto, COUNT(*) as total
        FROM indice_crimen
        WHERE causa IS NOT NULL AND causa != ''
        GROUP BY causa
        ORDER BY total DESC
        LIMIT 15
    """)
    por_tipo_acto = [dict(row) for row in cursor.fetchall()]
    
    # 2. Por Década
    cursor.execute("""
        SELECT (CAST(año as INTEGER) / 10 * 10) as decada, COUNT(*) as total
        FROM indice_crimen
        WHERE año IS NOT NULL AND CAST(año as INTEGER) > 0
        GROUP BY decada
        ORDER BY decada
    """)
    por_decada = [dict(row) for row in cursor.fetchall()]
    
    # 3. Top Partes (simulate "Apellidos")
    cursor.execute("""
        SELECT partes as apellido, COUNT(*) as total
        FROM indice_crimen
        WHERE partes IS NOT NULL 
          AND partes != ''
          AND LOWER(partes) NOT IN ('sumario', 'hojas sueltas', 'diligencias', 'interdicto de habeas corpus', 'juzgado del crimen')
          AND partes NOT LIKE '%Periódico%'
          AND partes NOT LIKE 'Escrituras públicas%'
          AND partes NOT LIKE 'Junta Protectora%'
        GROUP BY partes
        ORDER BY total DESC
        LIMIT 20
    """)
    top_apellidos = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return render_template('estadisticas.html', 
                         top_apellidos=top_apellidos, 
                         por_decada=por_decada, 
                         por_tipo_acto=por_tipo_acto)

if __name__ == '__main__':
    import os
    # Firebase/Render nos darán el puerto en una variable de entorno
    port = int(os.environ.get('PORT', 5000)) 
    print(f"🚀 INICIANDO APLICACIÓN EN PUERTO {port}")
    app.run(host='0.0.0.0', port=port)
