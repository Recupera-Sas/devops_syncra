import polars as pl
import glob
import os
from datetime import datetime


def process_management_files(input_path, output_path, partitions, process_data):

    print("=" * 80)
    print("🚀 STARTING MANAGEMENT FILES PROCESSING")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    if not os.path.exists(input_path):
        print(f"❌ ERROR: Input path does not exist: {input_path}")
        return

    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print("📁 Created output directory")

    # ==========================================================
    # BUSCAR TODOS LOS ARCHIVOS
    # ==========================================================

    files = []

    for ext in ("*.parquet", "*.parq", "*.csv"):
        files.extend(
            glob.glob(
                os.path.join(input_path, "**", ext),
                recursive=True
            )
        )

    print(f"\nArchivos encontrados: {len(files):,}")

    # ==========================================================
    # CARGAR GESTIONES (Lazy)
    # ==========================================================

    management_lfs = []

    print("\nBuscando archivos de gestión...")

    for file in files:

        try:

            if file.lower().endswith(".csv"):

                lf = pl.scan_csv(
                    file,
                    separator=";",
                    infer_schema_length=10000,
                    ignore_errors=True
                )

            else:

                lf = pl.scan_parquet(file)

            cols = lf.collect_schema().names()

            rename = {}

            # Compatibilidad con el script nuevo

            if "Campana" in cols:
                rename["Campana"] = "campana"

            if "Perfil" in cols:
                rename["Perfil"] = "perfil"

            if "Cuenta_Next" in cols:
                rename["Cuenta_Next"] = "cuenta_promesa"

            if rename:
                lf = lf.rename(rename)

            cols = lf.collect_schema().names()

            if all(
                c in cols
                for c in [
                    "campana",
                    "perfil",
                    "cuenta_promesa"
                ]
            ):

                print(" Gestión:", os.path.basename(file))

                management_lfs.append(

                    lf.select(

                        [

                            "campana",
                            "perfil",
                            "cuenta_promesa"

                        ]

                    )

                )

        except Exception:

            pass

    if not management_lfs:

        print("❌ No valid management data found")

        return

    lf_management = pl.concat(
        management_lfs,
        how="vertical_relaxed"
    )
    lf_management = lf_management.with_columns(
        pl.col("cuenta_promesa")
        .cast(pl.Utf8)
        .str.replace_all("-", "")
        .str.replace_all(r"\.", "")
        .str.strip_chars()
    )
    print("\nColumnas gestión:")
    print(lf_management.collect_schema().names())

    total_management = (
        lf_management
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )

    print(f"\n📊 Structure 1: {total_management:,} registros")

    # ==========================================================
    # CARGAR MAPEO
    # ==========================================================

    mapping_lfs = []

    print("\nBuscando archivos de mapeo...")

    for file in files:

        try:

            if file.lower().endswith(".csv"):

                lf = pl.scan_csv(
                    file,
                    separator=";",
                    infer_schema_length=10000,
                    ignore_errors=True
                )

            else:

                lf = pl.scan_parquet(file)

            cols = lf.collect_schema().names()

            rename = {}

            if "cuenta_promesa" in cols and "Cuenta_Next" not in cols:
                rename["cuenta_promesa"] = "Cuenta_Next"

            if "MARCA" in cols and "Marca_Asignada" not in cols:
                rename["MARCA"] = "Marca_Asignada"

            if rename:
                lf = lf.rename(rename)

            cols = lf.collect_schema().names()

            if "Cuenta_Next" in cols:

                print(" Mapeo:", os.path.basename(file))

                seleccionar = [
                    c
                    for c in [
                        "Cuenta_Next",
                        "Cuenta",
                        "Marca_Asignada"
                    ]
                    if c in cols
                ]

                mapping_lfs.append(

                    lf.select(seleccionar)

                )

        except Exception:

            pass

    if not mapping_lfs:

        print("❌ No mapping files found")

        return

    lf_mapping = pl.concat(
        mapping_lfs,
        how="vertical_relaxed"
    )
    print("\nColumnas mapeo:")
    print(lf_mapping.collect_schema().names())

    total_mapping = (
        lf_mapping
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )

    print(f"📊 Structure 2: {total_mapping:,} registros")

    # ==========================================================
    # LIMPIEZA
    # ==========================================================

    lf_management = lf_management.with_columns(

        pl.col("cuenta_promesa")
        .cast(pl.Utf8)
        .str.replace_all("-", "")
        .str.replace_all(r"\.", "")
        .str.strip_chars()

    )

    if "Cuenta" in lf_mapping.collect_schema().names():
        lf_mapping = lf_mapping.with_columns([
            pl.when(
                pl.col("Cuenta").is_null()
                | (pl.col("Cuenta").cast(pl.Utf8).str.strip_chars() == "")
            )
            .then(pl.lit("nan"))
            .otherwise(
                pl.col("Cuenta")
                .cast(pl.Utf8)
                .str.replace_all(r"\.0$", "")
                .str.strip_chars()
            )
            .alias("Cuenta_Real"),

            pl.col("Cuenta_Next")
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .str.replace_all(r"\.", "")
            .str.strip_chars()
        ])
    else:
        lf_mapping = lf_mapping.with_columns([
            pl.lit("nan").alias("Cuenta_Real"),

            pl.col("Cuenta_Next")
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .str.replace_all(r"\.", "")
            .str.strip_chars()
        ])

    lf_mapping = (

        lf_mapping

        .with_columns(

            pl.col("Cuenta_Next")
            .cast(pl.Utf8)
            .str.replace_all("-", "")
            .str.replace_all(r"\.", "")
            .str.strip_chars()

        )

        .unique(
            subset=["Cuenta_Next"],
            keep="first"
        )

    )
    # ==========================================================
    # JOIN
    # ==========================================================

    print("\nRealizando JOIN...")
    print("\nPrimeras cuentas gestión:")
    print(
        lf_management
        .select("cuenta_promesa")
        .limit(5)
        .collect(streaming=True)
    )

    print("\nPrimeras cuentas mapeo:")
    print(
        lf_mapping
        .select("Cuenta_Next")
        .limit(5)
        .collect(streaming=True)
    )
    print(
        lf_mapping
        .select([
            pl.col("Cuenta").is_null().sum().alias("Cuenta_null"),
            pl.col("Marca_Asignada").is_null().sum().alias("Marca_null"),
            pl.len().alias("Total")
        ])
        .collect(streaming=True)
    )

    lf_result = (

        lf_management.join(

            lf_mapping,

            left_on="cuenta_promesa",
            right_on="Cuenta_Next",

            how="inner"

        )

    )
    print(
        lf_result
        .limit(10)
        .collect(streaming=True)
    )

    stats = (

        lf_result

        .select(pl.len().alias("Total"))

        .collect(streaming=True)

    )

    total = stats["Total"][0]

    print(f"\n🔗 Records matched: {total:,}")

    if total == 0:

        print("❌ No matches found")

        return

    # ==========================================================
    # CLASIFICAR PERFIL
    # ==========================================================

    print("\nClasificando perfiles...")

    lf_result = (

        lf_result

        .with_columns(

            pl.col("perfil")
            .cast(pl.Utf8)
            .str.to_uppercase()
            .alias("perfil_upper")

        )

        .with_columns(

            pl.when(

                pl.col("perfil_upper").str.contains(
                    "CORREO|EMAIL|MAIL"
                )

            )

            .then(pl.lit("EMAIL"))

            .when(

                pl.col("perfil_upper").str.contains(
                    "BLASTER|IVR"
                )

            )

            .then(pl.lit("IVR"))

            .when(

                pl.col("perfil_upper").str.contains(
                    "MENSAJ|SMS|TEXTO"
                )

            )

            .then(pl.lit("SMS"))

            .otherwise(

                pl.col("perfil")

            )

            .alias("Recurso")

        )

    )

    # ==========================================================
    # AGRUPAR
    # ==========================================================

    print("\nAgrupando resultados...")
    lf_result = lf_result.with_columns([
        pl.col("Cuenta_Real").cast(pl.Utf8),
        pl.col("cuenta_promesa").cast(pl.Utf8),
    ])
    lf_group = (

        lf_result

        .group_by(

            [
                "Recurso",
                "campana",
                "Cuenta_Real",
                "cuenta_promesa",
                "Marca_Asignada"
            ]

        )
        .agg(
            pl.len().alias("Cantidad")
        )
        .rename(
            {
                "cuenta_promesa": "Cuenta_Sin_Punto",
                "Marca_Asignada": "Marca"
            }
        ).with_columns([
            pl.when(pl.col("Cuenta_Real") == "nan")
            .then(pl.col("Cuenta_Real"))
            .otherwise(pl.col("Cuenta_Real") + "-")
            .alias("Cuenta_Real"),

            pl.when(pl.col("Cuenta_Sin_Punto") == "nan")
            .then(pl.col("Cuenta_Sin_Punto"))
            .otherwise(pl.col("Cuenta_Sin_Punto") + "-")
            .alias("Cuenta_Sin_Punto"),
        ]).select(
            [
                "Cuenta_Real",
                "Cuenta_Sin_Punto",
                "Marca",
                "Recurso",
                "Cantidad"
            ]
        )
    )

    df_result = lf_group.collect(streaming=True)

    # ==========================================================
    # ESTADISTICAS
    # ==========================================================

    print("\n📊 Unique accounts by resource:")

    recursos = df_result["Recurso"].unique().to_list()

    for recurso in recursos:

        tmp = df_result.filter(

            pl.col("Recurso") == recurso

        )

        cuentas = tmp["Cuenta_Sin_Punto"].n_unique()

        cantidad = tmp["Cantidad"].sum()

        print(

            f"   {recurso}: {cuentas:,} unique accounts, {cantidad:,} total messages"

        )

    # ==========================================================
    # EXPORTAR
    # ==========================================================

    current_date = datetime.now().strftime("%Y%m%d")

    print("\n💾 Generating files:")

    for recurso in recursos:

        salida = os.path.join(

            output_path,

            f"{recurso}_{current_date}.csv"

        )

        (

            df_result

            .filter(

                pl.col("Recurso") == recurso

            )

            .write_csv(

                salida,

                separator=";"

            )

        )

        filas = (

            df_result

            .filter(

                pl.col("Recurso") == recurso

            )

            .height

        )

        total = (

            df_result

            .filter(

                pl.col("Recurso") == recurso

            )["Cantidad"].sum()

        )

        print(

            f"   ✅ {recurso}: {filas:,} rows, {total:,} total -> {os.path.basename(salida)}"

        )

    print("\n" + "=" * 80)

    print("✅ PROCESSING COMPLETED SUCCESSFULLY")

    print(f"📁 Files saved in: {output_path}")

    print("=" * 80)

    return df_result