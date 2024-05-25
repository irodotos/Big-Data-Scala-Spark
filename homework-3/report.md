### 1) What did you build?
I built a time series forecasting model to predict the future prices of Unleaded 95 OCT, Automotive Diesel, and Heating Gas Oil. The model uses historical data of these prices to make predictions for the next 365 days. The model is implemented using LSTM (Long Short-Term Memory) networks, which are a type of recurrent neural network well-suited for sequence data.

### 2) What technology did you choose?
I chose TensorFlow and Keras for building and training the LSTM model. Additionally, I used Python's pandas library for data manipulation and preprocessing, and scikit-learn's MinMaxScaler for data normalization.

### 3) How can we install the framework of your choice to run your code?
pip install -r requirements.txt

### 4) What was hard about building your application?
The challenging part was ensuring that the data preprocessing steps were correctly aligned with the requirements of the LSTM model, especially handling the sequence creation and ensuring that the shapes of the input and output data matched the model's expectations. Another challenge was to properly scale and inverse transform the data to maintain consistency in predictions.

### 5) What did you think was intuitive about the API/SDK that you used?
Keras, as part of TensorFlow, provides a very intuitive and high-level API for building and training neural networks. The sequential model setup and the layers API are straightforward, allowing for quick prototyping and testing of different model architectures.

### 6) Why do you think the developers made the API/SDK the way they did?
The developers of TensorFlow and Keras aimed to make deep learning accessible and easy to use for both beginners and experts. By providing a high-level API, they abstracted much of the complexity involved in building neural networks, allowing users to focus more on the model design and experimentation rather than low-level details. I am a beginner with zero knowledge and i build with with in 3 hours

### 7) What changes would you make to the API/SDK to improve it?
Improved documentation and examples specifically tailored for time series forecasting would be beneficial.

### 8) What did you learn in the process of building your application?
I learned how to preprocess and handle time series data for use in LSTM models, including sequence generation and normalization. I also gained a deeper understanding of LSTM networks and how they can be applied to forecasting problems. Additionally, I improved my skills in using TensorFlow and Keras for building and training deep learning models.

### 9) What was most surprising?
 The flexibility and power of TensorFlow and Keras in handling various types of neural network architectures were quite impressive.