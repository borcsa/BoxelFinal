# BoxelFinal
Final Boxel Project - Metastasis Detection


The goal of this project is to detect genes that can potentially be responsible for a higher/ lower tendency to metastasis. Also, it provides a model for prediction of likelihood of metastasis by individuals. Besides the correlated genes, the model takes factors as gender, age, smoking history, tumor size and tumor age.

Project Structure:

Part 1.: Preprocession of data
Part 2.: Detection of genes with highest correlation with metastasis (Pearsons correlation)
Part 3.: Building regression model with SVM / MLR
Part 4.: Building correlation network of genes
				(one for genes with positive correlation with metastasis and one with negative)
				
Program Arguments:

Input: 	1. path/to/clinical data
				2. path/to/file-sample-map
				3. path/to/genomic data

Output:	4. path/to/MLR Predictions
        5. path/to/SVM Predictions
				6. path/to/gene network (positive correlation with metastasis)
				7. path/to/gene network (negative correlation with metastasis)

About to Input:

The input data is to be downloaded from the following TCGA website (see link in the bottom of this file. To do so, you can use the data matrix. In that case, you will need to select the "Biotab" subcolumn of the "Clinical" column and the "RNASegV2" column for download. Among the clinical data, there is a file containing personal information about the patients including gender, age, smoking history, tumor size and tumor age. That will be the "clinical input". As for the "genomical input", we take the whole genomic data. Finally, there is a "file-sample-map" to be found in the root of the downloaded data. It describes which genomic data file belongs to which sample using its TCGA-barcode.

About the Output:
The gene networks are exported as gdf files readable by the graphic software Gephy. The SVM and MLR predictions were used for error analysis with R (see Crossvalidation.R in root).


TCGA website: https://tcga-data.nci.nih.gov/tcga/tcgaCancerDetails.jsp?diseaseType=LUAD&diseaseName=Lung%20adenocarcinoma
