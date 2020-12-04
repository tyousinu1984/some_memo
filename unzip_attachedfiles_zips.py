import zipfile

def _unzip_attachedfiles_zips(attachedfiles_PDFs, attachedfiles_ZIPs,
                              password):
    """
    zipファイルを解凍およびパスワード解除し、zip内に含まれるpdfを今回取得バケットに配置する。
    @param attachedfiles_PDFs: 添付PDFファイルリスト
    @param attachedfiles_ZIPs: 添付ZIPファイルリスト
    @param password: password
    @return extract_flag : 解凍成功かどうか
    """
    logger.info("_unzip_attachedfiles_zips start")
    extract_flag = None
    for zip_file in attachedfiles_ZIPs:
        zip_name, zip_content = zip_file["file_name"], zip_file["file_content"]
        extract_flag, extract_results = _unzip_zip(unique_name__activity_name__execute_datetime,
                                                   zip_name, zip_content, password)
        if extract_flag == "OK":
            attachedfiles_PDFs.extend(extract_results)
        else:
            return extract_flag

    logger.info("_unzip_attachedfiles_zips end")
    return extract_flag


def _unzip_zip(unique_name__activity_name__execute_datetime,
               zip_name, zip_content, password):
    """
    zipファイルを解凍およびパスワード解除し、zip内に含まれるpdfを今回取得バケットに配置する。
    @param zip_name: 添付ファイル名
    @param zip_content: 添付ファイル本体
    @param password: password
    """
    extract_results = []
    logger.info("_unzip_put_pdf start")
    # file streamに変換
    data = io.BytesIO(zip_content)
    # Library"zipfile"で読込
    data = zipfile.ZipFile(data, "r")

    # パスワード解除フラグの初期設定
    extract_flag = "OK"
    # zipファイル内に存在するファイルに対して処理を実施。
    for info in data.infolist():
        # 拡張子が「pdf」の場合のみ処理を行う。
        data_in_zip = None
        if os.path.splitext(info.filename)[1][1:] == "pdf":
            # ファイルが取り出せない場合に、try-exceptで、パスワードをセット。
            try:
                if extract_flag == "OK":
                    # zipファイル内のファイルを1つ読み込む
                    data_in_zip = data.read(info.filename)

            # パスワードが掛かっている場合
            except RuntimeError:
                try:
                    # パスワードをセットして、ファイルの読み込み
                    data.setpassword(password.encode("ascii"))
                    data_in_zip = data.read(info.filename)
                except RuntimeError:
                    extract_flag = "NG"
                    logger.info(
                        unique_name__activity_name__execute_datetime
                        + ":" + zip_name +
                        " password " + password + " NG")
                    break
            # zip内のファイルが1つでも読み込めた場合にこの箇所の処理をする。

            if extract_flag == "OK":
                path = info.filename
                path = path.encode("cp437")
                path = path.decode("cp932")
                # zip内にさらにフォルダがあった場合に、pdfのファイル名だけ取得する。
                path = path.split('/')[-1]
                extract_results.append({"file_name": path, "file_content": data_in_zip})

    logger.info("_unzip_put_pdf end")
    return extract_flag, extract_results
