# Estado del Arte — Ingeniería de Datos
### Prácticas Pre Profesionales · Universidad Técnica Particular de Loja

---

| | |
|---|---|
| **Institución** | Universidad Técnica Particular de Loja (UTPL) |
| **Departamento** | Departamento de Ingenierías y Arquitectura |
| **Carrera** | Computación |
| **Énfasis** | Ingeniería de Datos |
| **Autor** | Jesús Alejandro Rivas Reinoso |
| **Tutor** | Ing. Francisco Oswaldo Vargas Naula |
| **Período** | Abril 2026 – Agosto 2026 |

---

## Resumen

El volumen de datos generados en entornos digitales ha crecido de manera exponencial, impulsado por el auge de internet, los dispositivos IoT, las redes sociales y la inteligencia artificial. Ante este escenario, la ingeniería de datos emerge como disciplina fundamental para diseñar, construir y mantener sistemas que permitan extraer valor real de la información.

Este Estado del Arte examina las herramientas, arquitecturas y tendencias predominantes en la industria para la gestión de flujos de datos a gran escala, con énfasis en los modelos ETL y ELT, los *pipelines* de datos y su orquestación.

> **Palabras clave:** Ingeniería de datos, pipeline de datos, ETL, ELT, Apache Spark, orquestación, inteligencia artificial, arquitectura de datos, cloud computing.

---

## Abstract

The volume of data generated in digital environments has grown exponentially, driven by the rise of the internet, IoT devices, social networks, and artificial intelligence. Data engineering emerges as a fundamental discipline for designing, building, and maintaining systems that enable organizations to extract real value from information. This State of the Art examines the predominant tools, architectures, and industry trends for managing large-scale data flows, with emphasis on ETL and ELT models, data pipelines, and their orchestration.

> **Keywords:** Data engineering, data pipeline, ETL, ELT, Apache Spark, orchestration, artificial intelligence, data architecture, cloud computing.

---

## Tabla de Contenidos

- [Introducción](#introducción)
- [Marco Conceptual](#marco-conceptual)
  - [¿Qué es la Ingeniería de Datos?](#qué-es-la-ingeniería-de-datos)
  - [¿Qué es un Pipeline de Datos?](#qué-es-un-pipeline-de-datos)
  - [Modelo ETL vs ELT](#modelo-etl-vs-elt)
- [Trabajos Relacionados](#trabajos-relacionados)
- [Herramientas y Tecnologías](#herramientas-y-tecnologías)
- [Tendencias y Futuro](#tendencias-y-futuro-20262028)
- [Conclusiones](#conclusiones)
- [Referencias](#referencias)

---

## Introducción

Los datos han tomado más relevancia que nunca, tanto en empresas como en aplicaciones que día a día trabajan con información. A medida que la tecnología se desarrolla y el uso de dispositivos digitales se facilita en todo el mundo, el volumen de datos producidos aumenta considerablemente — impulsado por el crecimiento de internet, las redes sociales, los dispositivos IoT, el comercio electrónico y la computación en la nube.

Con el *"Boom"* de la IA, la importancia de los datos se vuelve aún más evidente. Los algoritmos de inteligencia artificial son herramientas potentes capaces de procesar grandes volúmenes de información, aprender patrones y tomar decisiones autónomas; sin embargo, este desempeño está estrictamente relacionado con la calidad de la información que se les entrega:

> *Es mejor 1 archivo con datos limpios y claros que 100 archivos desordenados con información incompleta.*

Esto plantea uno de los mayores retos técnicos actuales: **la escalabilidad y orquestación del flujo de datos**. Las bases de datos tradicionales y las extracciones manuales ya no son suficientes. El verdadero desafío radica en construir *pipelines* que sean eficientes, tolerantes a fallos y que mantengan la calidad de la información desde su origen hasta su consumo final.

**Preguntas de investigación:**
- ¿Cuáles son las herramientas predominantes en la industria para la extracción, transformación y orquestación de datos a gran escala?
- ¿Cómo están aplicando otras instituciones estas tecnologías para modernizar sus ecosistemas analíticos?

---

## Marco Conceptual

### ¿Qué es la Ingeniería de Datos?

Según **Databricks**, la ingeniería de datos se define como:

> *"La práctica de diseñar, construir y mantener sistemas que recopilan, almacenan, transforman y entregan datos para su análisis, generación de informes, aprendizaje automático y toma de decisiones. Se trata de asegurarse de que los datos realmente lleguen a tiempo y en buen estado."*

### ¿Qué es un Pipeline de Datos?

Según **Amazon Web Services**:

> *"Una canalización de datos consiste en una serie de pasos de procesamiento dirigidos a preparar los datos empresariales para llevar a cabo análisis [...] Las canalizaciones de datos bien organizadas facilitan diversos proyectos de big data, como las visualizaciones de datos, los análisis exploratorios de datos y las tareas de machine learning."*

### Modelo ETL vs ELT

#### ETL — Extraer, Transformar, Cargar *(modelo tradicional)*

```
[Fuente de datos] --> EXTRAE --> TRANSFORMA (servidor secundario) --> CARGA (base de datos destino)
```

El enfoque ETL utiliza reglas empresariales para procesar datos de varios orígenes **antes** de la integración centralizada.

#### ELT — Extraer, Cargar, Transformar *(modelo moderno)*

```
[Fuente de datos] --> EXTRAE --> CARGA (cloud warehouse) --> TRANSFORMA (dentro del almacén)
```

El enfoque ELT carga los datos tal como están y los transforma en una etapa posterior, aprovechando la potencia de la nube. Según AWS, *"el proceso de ELT es más rápido que el de ETL y se ha convertido en la norma en la actualidad"*.

> **¿Por qué importa este cambio?** El ELT permite gestionar todo tipo de información, incluyendo **datos no estructurados**, utilizando la paralelización de los almacenes modernos para ofrecer transformaciones en tiempo real. Los equipos técnicos pueden enfocarse en el análisis en lugar de invertir todo su esfuerzo en la transformación previa.

---

## Trabajos Relacionados

### Caso 1 — Smart Campus: Estimación de ocupación con datos WiFi

**Hernando-Cánovas et al. (2025)** desarrollaron un sistema predictivo para estimar la ocupación física en una biblioteca universitaria utilizando **datos anónimos de conexiones WiFi**.

- **Flujo:** Extracción continua de registros de puntos de acceso inalámbrico → procesamiento de variables → modelos de machine learning
- **Resultado:** Sistema robusto, rentable y que preserva la privacidad del usuario

> *"La estimación de ocupación basada en WiFi ofrece una solución robusta, rentable y que preserva la privacidad para implementaciones en el mundo real."* — Hernando-Cánovas et al., 2025

---

### Caso 2 — Industria Financiera: Integración de sistemas heredados en la nube

**Ahonen (2025)** desarrolló una arquitectura para integrar datos desde sistemas heredados hacia plataformas modernas en la nube, utilizando:

- **Apache Spark Structured Streaming** para la captura de eventos continuos
- **Azure Event Hubs** para la captura de cambios en bases de datos
- **Databricks** para el procesamiento y fusión de registros en tiempo real

| Métrica | Resultado |
|---|---|
| Tiempo de respuesta | 1.2 – 1.4 segundos |
| Enfoque | Structured Streaming en Databricks |

> *"El streaming estructurado en Databricks es un enfoque viable y de alto rendimiento para pipelines de datos en tiempo real en entornos empresariales."* — Ahonen, 2025

---

## Herramientas y Tecnologías

| Herramienta | Función Principal | Característica Destacada |
|---|---|---|
| **Apache Spark** | Procesamiento distribuido (Batch/Streaming) | Motor clave para analítica masiva y flujos de baja latencia con Structured Streaming |
| **Python / Scala** | Desarrollo de lógica y limpieza de datos | Lenguajes versátiles para el manejo de datasets complejos y modelos de machine learning |
| **Databricks** | Plataforma de analítica unificada | Entorno en la nube que optimiza la ejecución de Spark para entornos empresariales |
| **Azure Event Hubs** | Ingestión de datos en tiempo real | Servicio de transmisión de eventos de alta capacidad para capturar datos de sistemas heredados |
| **dbt (Data Build Tool)** | Transformación en el almacén (ELT) | Permite transformar datos crudos en tablas analíticas usando únicamente SQL versionado |
| **Prefect / Airflow** | Orquestación de flujos de trabajo | Coordinación automatizada de tareas, manejo de reintentos y monitoreo de dependencias |
| **Snowflake** | Almacenamiento de datos (Cloud Warehouse) | Repositorio columnar escalable que permite separar el almacenamiento del cómputo |
| **Power BI / Tableau** | Visualización y BI | Transformación de datos procesados en indicadores interactivos para la toma de decisiones |

---

## Tendencias y Futuro (2026–2028)

### 1. IA Agéntica y "AI-Ready Data"

La tendencia principal es el paso de simples tuberías de datos a **"tuberías de contexto"**. La ingeniería de datos ya no solo busca mover tablas, sino preparar datos específicamente para ser consumidos por **agentes de IA autónomos**. Esto implica que los *pipelines* modernos deben generar metadatos ricos y *embeddings* (vectores) de forma nativa, asegurando que la IA tenga un contexto confiable para actuar sin intervención humana constante.

### 2. Observabilidad de Datos y "Governance as Code"

A medida que los flujos se vuelven más complejos, la prioridad ha pasado del rendimiento a la **confianza total**. La tendencia es integrar la observabilidad de datos como un estándar dentro del *stack* tecnológico, monitoreando de forma automática la frescura, calidad y linaje de los datos, y detectando anomalías o registros dañados mediante algoritmos predictivos antes de que afecten los tableros de control o los modelos de decisión.

### 3. Edge Data Engineering

Impulsado por el crecimiento de dispositivos IoT y redes WiFi inteligentes, el procesamiento se está desplazando hacia el **"borde" (*Edge Computing*)**. Se estima que para finales de 2026, gran parte de los datos empresariales se procesará cerca de la fuente de origen, permitiendo filtrar y transformar información de alta frecuencia en tiempo real antes de enviarla a la nube — reduciendo costos de latencia y almacenamiento, algo fundamental para aplicaciones de seguridad y monitoreo de infraestructura institucional.

---

## Conclusiones

| # | Conclusión |
|---|---|
| 1 | **Superioridad del ELT:** El modelo Extraer-Cargar-Transformar es la opción más eficiente para la nube, reduciendo tiempos de carga y permitiendo procesar datos crudos con mayor flexibilidad que el método tradicional. |
| 2 | **Vitalidad de la Orquestación:** Herramientas como Prefect o Airflow son el motor de confianza del sistema. Sin automatización de tareas y monitoreo de errores, escalar un flujo de datos sin riesgo de colapso no es viable. |
| 3 | **Utilidad Práctica:** Los casos de estudio (como el de redes WiFi universitarias) confirman que la ingeniería de datos transforma registros técnicos simples en inteligencia operativa, permitiendo optimizar recursos y espacios físicos de forma real. |
| 4 | **Preparación para la IA:** El futuro exige *pipelines* que entreguen datos listos para IA. La prioridad es ahora la **calidad y la observabilidad** para alimentar modelos autónomos con información verídica en tiempo real. |

---

## Referencias

- Databricks. (s.f.). *¿Qué es la ingeniería de datos?* Recuperado el 7 de mayo de 2026, de https://www.databricks.com/es/blog/what-is-data-engineering

- Amazon Web Services. (s.f.). *¿Qué es una canalización de datos?* https://aws.amazon.com/es/what-is/data-pipeline/

- Amazon Web Services. (s.f.). *¿Cuál es la diferencia entre los procesos de ETL y ELT?* Recuperado el 7 de mayo de 2026, de https://aws.amazon.com/es/compare/the-difference-between-etl-and-elt/

- Hernando-Cánovas, L., Martínez-Sala, A. S., Sánchez-Aarnoutse, J. C., & Alcaraz, J. J. (2025). A Machine Learning Approach for Estimating Person Counts Using Anonymous WiFi Data in a University Library. *Sensors, 25*(22), 7065. https://doi.org/10.3390/s25227065

- Ahonen, T. (2025). *Real-time data pipeline architecture for financial services* [Tesis]. Databricks Structured Streaming.

- Bismart. (2026). *Tendencias de datos 2026 que toda empresa debe conocer.* https://blog.bismart.com/tendencias-datos-2026-empresas

- CDO Magazine. (2026). *Why 2026 Will Redefine Data Engineering as an AI-Native Discipline.* https://www.cdomagazine.tech/opinion-analysis/why-2026-will-redefine-data-engineering-as-an-ai-native-discipline

- DataGrowth. (2026). *Las 25 Tendencias de Datos que Impulsarán las Empresas en 2026.* https://datagrowth.es/tendencias-datos-2026-empresas/

---
