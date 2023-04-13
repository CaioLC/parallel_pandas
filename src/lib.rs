use pyo3::{prelude::*, types::{PyString, PyTuple, PyIterator, PyFunction, PyDict, PyBytes}};
use std::{collections::{HashMap, hash_map::DefaultHasher}, error::Error, sync::{Mutex, RwLock}, borrow::Borrow, hash::Hash};
use std::sync::{Arc, mpsc::channel};

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// receives a python function and call it
#[pyfunction]
fn returns_text() -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let fun: Py<PyAny> = PyModule::from_code(
            py,
            "def example():
                return 'calling python from rust!'
            ",
            "",
            "",
        )?
        .getattr("example")?
        .into();
       Ok(fun.call(py, (), None)?) 
    })

}

#[pyfunction]
fn sum_numbers(py: Python<'_>, numbers: Vec<u32>) -> PyResult<u32> {
    // We release the GIL here so any other Python threads get a chance to run.
    py.allow_threads(move || {
        // An example of an "expensive" Rust calculation
        let sum = numbers.iter().sum();

        Ok(sum)
    })
}

#[pyfunction]
fn unsafe_with_nested_gil() -> PyResult<()> {
    Python::with_gil(|py| -> PyResult<()> {
        for _ in 0..10 {
            let pool = unsafe { py.new_pool() };
            let py = pool.python();
            let hello: &PyString = py.eval("\"Hello World!\"", None, None)?.extract()?;
            println!("Python says: {}", hello);
        }
        Ok(())
    })
}

#[pyfunction]
fn type_me(py: Python<'_>, obj: PyObject) -> PyResult<PyObject> {
    let fun: Py<PyAny> = PyModule::from_code(py,
    "def exp(x):
        return type(x)
    ",
        "", "")?.getattr("exp")?.into();
    let py_tuple = PyTuple::new(py, &[obj]);
    fun.call(py, py_tuple, None)
}

#[pyfunction]
fn any_function(py: Python<'_>, fun: Py<PyAny>) -> PyResult<PyObject> {
    fun.call0(py) 
}

#[pyfunction]
fn iterable(iter_obj: Vec<String>) -> PyResult<()> {
    for line in iter_obj {
        println!(
            "{}", line
        )
    }
   Ok(())
}

#[pyfunction]
fn from_generator(gen_obj: &PyIterator) {
    println!("{:?}", gen_obj.count());
}

#[pyfunction]
fn par_apply(py: Python, data: PyObject) -> PyResult<PyObject> {
    // Convert PyObject to HashMap
    let dict = PyDict::from_sequence(py, data)?;
    let mut hash_map = HashMap::new();
    for (key, value) in dict.iter(){
        let key_str = key.to_string();
        let value_pickle = value.extract::<&[u8]>()?;
        hash_map.insert(key_str, value_pickle);
    }
    
    // Spawn separate threads to call python function

    let (tx, rx) = channel();
    std::thread::scope(|scope| {

        for (_, value) in hash_map.iter() {
            let data = Arc::new(*value);
            let tx = tx.clone();
            scope.spawn(move || {
                let data_local = data.clone();
                let mut py_gil;
                unsafe { py_gil = Python::assume_gil_acquired(); }; 
                let pickle = PyBytes::new(py_gil, &data_local);
                let fun: Py<PyAny> = PyModule::from_code(py_gil,
                    "def exp(x):
                        return type(x)
                    ",
                        "", "").unwrap().getattr("exp").unwrap().into();
                let result = fun.call1(py_gil, (pickle,)).unwrap().extract::<Vec<u8>>(py_gil).unwrap();
                // Send result back to main thread
                tx.send(result).unwrap();
                });
    
        }
    });

    let mut python_dict = PyDict::new(py);
    for (key, _) in dict.iter() {
        dict.set_item(key, rx.recv().unwrap());
    }
    Ok(dict.into())

}

fn from_py_mapping(py: &Python, data: &PyObject) -> Result<HashMap<String, Vec<f64>>, PyErr> {
    todo!()
}

/// A Python module implemented in Rust.
#[pymodule]
fn parallel_pandas(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(returns_text, m)?)?;
    m.add_function(wrap_pyfunction!(sum_numbers, m)?)?;
    m.add_function(wrap_pyfunction!(unsafe_with_nested_gil, m)?)?;
    m.add_function(wrap_pyfunction!(type_me, m)?)?;
    m.add_function(wrap_pyfunction!(any_function, m)?)?;
    m.add_function(wrap_pyfunction!(iterable, m)?)?;
    m.add_function(wrap_pyfunction!(from_generator, m)?)?;
    Ok(())
}