
function uploadAndMergePDFs() {
    const formData = new FormData();
    const files = document.querySelector('#pdfFiles').files;
    for (let i = 0; i < files.length; i++) {
        formData.append('files', files[i]);
    }

    axios.post(`${rotaIntranet}/comprimido_pdf`, formData, {
        responseType: 'blob'  // Importante para lidar com o tipo de conteúdo de resposta como arquivo binário
    })
        .then(response => {
            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', 'pdf_junto.pdf');  // Nome do arquivo para download
            document.body.appendChild(link);
            link.click();
            link.parentNode.removeChild(link);  // Remove o link após o download
        })
        .catch(error => console.error('Erro ao fazer upload e merge dos PDFs:', error));
}


document.addEventListener('DOMContentLoaded', () => {

    const btnbaixar = document.getElementById('btnbaixar');
    if (btnbaixar) {
        btnbaixar.addEventListener('click', uploadAndMergePDFs);
    }

    // Listener para abrir o seletor de arquivos quando o botão é clicado
    const selectFilesButton = document.getElementById('selectFilesButton');
    if (selectFilesButton) {
        selectFilesButton.addEventListener('click', () => {
            document.getElementById('pdfFiles').click();
        });
    }

    // Listener para atualizar a contagem de arquivos selecionados
    document.getElementById('pdfFiles').addEventListener('change', function() {
        const fileCount = this.files.length;
        document.getElementById('file-count').textContent = `${fileCount} arquivo(s) selecionado(s)`;
    });
});