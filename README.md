# Intro
Web scraping é uma prática que está se tornando cada vez mais comum. Em especial, há uma demanda bem alta por profissionais capazes de realizar essa atividade no mercado financeiro brasileiro. Criei esse repositório como uma demonstração relativamente simples porém bem eficiente de scraping.

# Sobre o código
O código em si está dividido nos 4 scripts:
- interface_with_cvm: permite a obtenção dos dados da cvm
- process_data: pega os dados da interface_with_cvm e processa eles
- email_module: realiza o envio dos emails
- core: reúne os outros 3 (é ele que você deve executar)

O exploracao.ipynb apresenta as requests (obtidas inspecionando o site usando as devtools do Chrome) usadas para a extração dos dados, enquanto
o prototipo.ipynb é um rascunho do projeto

# Sobre as bibliotecas
- Lembre-se de instalar os requisitos da requirements.txt
- Preferi não usar o selenium por ele ser muito pesado e ter maior chance de "quebrar"